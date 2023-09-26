use anyhow::anyhow;
use chrono::{Utc, TimeZone, Duration};
use serenity::builder::CreateEmbed;
use serenity::model::prelude::command::CommandOptionType;
use serenity::model::prelude::{Interaction, InteractionResponseType, Presence, ActivityType, Activity, UserId};
use serenity::model::user::User;
use serenity::utils::Colour;
use serenity::{async_trait, model::prelude::GuildId};
use sqlx::{query, Row, PgPool};
use shuttle_service::ResourceBuilder;
use sqlx::postgres::PgRow;
use serenity::model::gateway::Ready;
use serenity::prelude::*;
use shuttle_secrets::SecretStore;
use tracing::info;
use std::time::{SystemTime, UNIX_EPOCH};
use std::convert::TryFrom;


struct Bot {
    pool: PgPool
}

impl Bot {
    async fn save_session(&self, user_id: &i64) {
        let row = query("SELECT game_id, starttime FROM game_sessions WHERE user_id=$1;")
                                            .bind(user_id)
                                            .fetch_optional(&self.pool).await.unwrap();
        if row.is_none() {
            return;
        }
        info!("Saving {:?}'s session", user_id);
        let row: PgRow = row.unwrap();
        let game_id: i64 = row.get::<i64, usize>(0);
        let starttime: i64 = row.get::<i64, usize>(1);
        let currenttime: i64 = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()).unwrap();
        let playtime: i64 = currenttime - starttime;
        info!("Playtime: {:?}s", playtime);
        self.add_playtime(user_id, &game_id, &playtime).await;
    }
    
    async fn get_summary(&self, user: &User) -> CreateEmbed {

        let user_id = i64::try_from(*user.id.as_u64()).unwrap();
        let mut embed = CreateEmbed::default()
            .colour(Colour::TEAL)
            .title(format!("{}'s playtime summary", user.name)).to_owned();

        for row in query("SELECT name, playtime FROM game_entries NATURAL JOIN games WHERE user_id=$1 ORDER BY playtime DESC LIMIT 10;")
                                            .bind(user_id)
                                            .fetch_all(&self.pool).await.unwrap() {
            let game_name: &str = row.get::<&str, usize>(0);
            let playtime = Duration::seconds(row.get::<i64, usize>(1));
            let tmp_datetime = Utc.with_ymd_and_hms(1337, 1, 1, 0, 0, 0).unwrap() + playtime;
            let formated_playtime = tmp_datetime.format("%X").to_string();
            embed.field(game_name, formated_playtime, true);
        }
        return embed;
    }
    
    async fn is_game_in_db(&self, game_name: &String) -> bool {
        let row = query("SELECT * FROM games WHERE name=$1;")
                                            .bind(game_name)
                                            .fetch_optional(&self.pool).await.unwrap();
        return row.is_some();
    }
    
    async fn register_session(&self, user_id: &i64, game_name: &String, starttime: &i64) {
        if !self.is_game_in_db(game_name).await {
            info!("Adding {:?} to db", game_name);
            self.add_game(game_name).await;
        }
        info!("Registering {:?}'s session", user_id);
        let game_id: i64 = self.get_game_id(game_name).await;
        query("INSERT INTO game_sessions (user_id, game_id, starttime) VALUES ($1, $2, $3);")
            .bind(user_id)
            .bind(game_id)
            .bind(starttime)
            .execute(&self.pool).await.unwrap();
    }
    
    async fn get_game_id(&self, game_name: &String) -> i64 {
        let row = query("SELECT game_id FROM games WHERE name=$1;")
                                            .bind(game_name)
                                            .fetch_one(&self.pool).await.unwrap();
        return row.get::<i64, usize>(0);
    }
    
    async fn add_playtime(&self, user_id: &i64, game_id: &i64, playtime: &i64) {
        let row = query("SELECT * FROM game_entries WHERE user_id=$1 AND game_id=$2;")
                                            .bind(user_id)
                                            .bind(game_id)
                                            .fetch_optional(&self.pool).await.unwrap();
        if row.is_none() {
            query("INSERT INTO game_entries (user_id, game_id, playtime) VALUES ($1, $2, $3);")
                .bind(user_id)
                .bind(game_id)
                .bind(playtime)
                .execute(&self.pool).await.unwrap();
        } else {
            query("UPDATE game_entries SET playtime=playtime+$1 WHERE user_id=$2 AND game_id=$3;")
                .bind(playtime)
                .bind(user_id)
                .bind(game_id)
                .execute(&self.pool).await.unwrap();
        }
    }
    
    async fn add_game(&self, game_name: &String) {
        query("INSERT INTO games (name) VALUES ($1);")
            .bind(game_name)
            .execute(&self.pool).await.unwrap();
    }
    
    async fn build_db(&self) {
        query(
            "CREATE TABLE IF NOT EXISTS games (
                game_id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );").execute(&self.pool).await.unwrap();
        query(
            "CREATE TABLE IF NOT EXISTS game_entries (
                user_id BIGINT NOT NULL,
                game_id BIGINT NOT NULL,
                playtime BIGINT NOT NULL,
                PRIMARY KEY (user_id, game_id),
                FOREIGN KEY (game_id) REFERENCES games(game_id)
            );").execute(&self.pool).await.unwrap();
        query(   
            "CREATE TABLE IF NOT EXISTS game_sessions (
                user_id BIGINT NOT NULL,
                game_id BIGINT NOT NULL,
                starttime BIGINT NOT NULL,
                PRIMARY KEY (user_id, game_id),
                FOREIGN KEY (game_id) REFERENCES games(game_id)
            );").execute(&self.pool).await.unwrap();
        query( 
            "DELETE FROM game_sessions;"
        ).execute(&self.pool).await.unwrap();
        query(
            "CREATE OR REPLACE FUNCTION remove_session()
                RETURNS TRIGGER 
                AS
                $$
                BEGIN
                    DELETE FROM game_sessions WHERE user_id = NEW.user_id AND game_id = NEW.game_id;
                    RETURN NEW;
                END;
            $$ LANGUAGE plpgsql;"
        ).execute(&self.pool).await.unwrap();
        query(
            "CREATE OR REPLACE TRIGGER trigger_clear_sessions
                AFTER INSERT ON game_entries
                FOR EACH ROW
                EXECUTE PROCEDURE remove_session();"
        ).execute(&self.pool).await.unwrap();
    }

    async fn resetall(&self) {
        query("DELETE FROM game_entries;").execute(&self.pool).await.unwrap();
        query("DELETE FROM game_sessions;").execute(&self.pool).await.unwrap();
        query("DELETE FROM games;").execute(&self.pool).await.unwrap();
    }

    async fn reset(&self, user_id: &i64) {
        query("DELETE FROM game_entries WHERE user_id=$1;")
            .bind(user_id)
            .execute(&self.pool).await.unwrap();
        query("DELETE FROM game_sessions WHERE user_id=$1;")
            .bind(user_id)
            .execute(&self.pool).await.unwrap();
    }

    async fn hardreset(&self) {
        self.resetall().await;
        query("DROP TABLE game_entries;").execute(&self.pool).await.unwrap();
        query("DROP TABLE game_sessions;").execute(&self.pool).await.unwrap();
        query("DROP TABLE games;").execute(&self.pool).await.unwrap();
        self.build_db().await;
    }
}

#[async_trait]
impl EventHandler for Bot {

    async fn ready(&self, ctx: Context, ready: Ready) {
        info!("{} is connected!", ready.user.name);
        let guild_id = GuildId(1063039820575801385);
        self.build_db().await;

        GuildId::set_application_commands(&guild_id, &ctx.http, |commands| {
            commands
                .create_application_command(|command| { command.name("summarize").description("Shows the 10 most played games of a user") 
                    .create_option(|option| {option.name("user").description("The target").kind(CommandOptionType::User).required(true)}) })
                .create_application_command(|command| { command.name("reset").description("Resets the player's playtimes") 
                    .create_option(|option| {option.name("user").description("The target").kind(CommandOptionType::User).required(true)}) })
                .create_application_command(|command| { command.name("resetall").description("Resets all playtimes and games")})
                .create_application_command(|command| { command.name("hardreset").description("Destroys the database")})  
        }).await.unwrap();
    }

       // `interaction_create` runs when the user interacts with the bot
    async fn interaction_create(&self, ctx: Context, interaction: Interaction) {
        // check if the interaction is a command
        if let Interaction::ApplicationCommand(command) = interaction {

             match command.data.name.as_str() {
                "summarize" => async { 
                    let user_id = command.data.options[0].value.as_ref().unwrap().as_str().unwrap().parse::<u64>().unwrap(); 
                    let user = UserId(user_id).to_user(&ctx.http).await.unwrap();
                    let embed = self.get_summary(&user).await;
                    command.create_interaction_response(&ctx.http, |response| {
                        response
                            .kind(InteractionResponseType::ChannelMessageWithSource)
                            .interaction_response_data(|message| message.set_embed(embed))
                    })
                        .await.expect("Cannot respond to slash command");
                }.await,
                "reset" => async {
                    let mut message_str = "You don't have the permission to use this command.".to_string();
                    if command.user.id.to_string() == "618355400038940682" {
                        let user_id = command.data.options[0].value.as_ref().unwrap().as_str().unwrap().parse::<u64>().unwrap(); 
                        let user = UserId(user_id).to_user(&ctx.http).await.unwrap();
                        self.reset(&i64::try_from(*user.id.as_u64()).unwrap()).await;
                        message_str = format!("Successfully reseted {}'s playtimes.", user.mention());
                    }
                    
                    command.create_interaction_response(&ctx.http, |response| {
                        response
                            .kind(InteractionResponseType::ChannelMessageWithSource)
                            .interaction_response_data(|message| message.ephemeral(true).content(message_str))
                    })
                        .await.expect("Cannot respond to slash command");
                }.await,
                "resetall" => async {
                    let mut message_str = "You don't have the permission to use this command.".to_string();
                    if command.user.id.to_string() == "618355400038940682" {
                        self.resetall().await;
                        message_str = "Successfully reseted all playtimes and games.".to_string();
                    }
                    command.create_interaction_response(&ctx.http, |response| {
                        response
                            .kind(InteractionResponseType::ChannelMessageWithSource)
                            .interaction_response_data(|message| message.ephemeral(true).content(message_str))
                    })
                        .await.expect("Cannot respond to slash command");
                }.await,
                "hardreset" => async {
                    let mut message_str = "You don't have the permission to use this command.".to_string();
                    if command.user.id.to_string() == "618355400038940682" {
                        self.hardreset().await;
                        message_str = "Successfully reconstructed the database".to_string();
                    }
                    command.create_interaction_response(&ctx.http, |response| {
                        response
                            .kind(InteractionResponseType::ChannelMessageWithSource)
                            .interaction_response_data(|message| message.ephemeral(true).content(message_str))
                    })
                        .await.expect("Cannot respond to slash command");
                }.await,
                command => unreachable!("Command don't have a handler: {}", command),
            };
        }
    }

    async fn presence_update(&self, _ctx: Context, new_data: Presence) {
        let user_id = i64::try_from(*new_data.user.id.as_u64()).unwrap();
        if new_data.activities.len() == 0 {
            self.save_session(&user_id).await;
            return;
        }
        let user_activity: &Activity = &new_data.activities[0];
        let game_name: &String = &user_activity.name;
        if user_activity.kind == ActivityType::Playing {
            let starttime = i64::try_from(std::time::Duration::from_millis(user_activity.timestamps.as_ref().unwrap().start.unwrap()).as_secs()).unwrap();
            self.register_session(&user_id, game_name, &starttime).await;
        }
    }

    
}


#[shuttle_runtime::main]
async fn serenity(
    #[shuttle_secrets::Secrets] secret_store: SecretStore, #[shuttle_shared_db::Postgres] pool: PgPool,
) -> shuttle_serenity::ShuttleSerenity {
    // Get the discord token set in `Secrets.toml`
    let token = if let Some(token) = secret_store.get("DISCORD_TOKEN") {
        token
    } else {
        return Err(anyhow!("'DISCORD_TOKEN' was not found").into());
    };
    // Set gateway intents, which decides what events the bot will be notified about
    let intents = GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT | GatewayIntents::GUILD_PRESENCES;
    let client = Client::builder(&token, intents)
        .event_handler(Bot{pool})
        .await
        .expect("Err creating client");

    Ok(client.into())
}
