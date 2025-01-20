CREATE TABLE events(
		event_id UUID PRIMARY KEY,
		category_id INTEGER NOT NULL,
		commence_time TIMESTAMP NOT NULL,
		description VARCHAR (100),
		home INTEGER,
		away INTEGER,
		homeScore INTEGER,
		awayScore INTEGER
		);

CREATE TABLE event_urls(
		bookmaker_key VARCHAR(50) NOT NULL,
		event_url VARCHAR(100),
		event_id UUID NOT NULL,
		oghome VARCHAR(100),
		ogaway VARCHAR(100),
		PRIMARY KEY (bookmaker_key, event_id)
);

CREATE TABLE bookmaker_categories(
		bookmaker_key VARCHAR(50) NOT NULL,
		category_url VARCHAR(100),
		category_bookmaker_key VARCHAR(50),
		text VARCHAR(50),
		category_id INTEGER,
		bookmaker_category_id SERIAL PRIMARY KEY NOT NULL,
);

CREATE TABLE bet_types(
		bet_type_id SERIAL PRIMARY KEY NOT NULL,
		key VARCHAR (100) UNIQUE NOT NULL
		);      

CREATE TABLE bet_type_dict(
		bet_type_id INTEGER NOT NULL,
		text VARCHAR (100) PRIMARY KEY UNIQUE NOT NULL
		);    

CREATE TABLE team_dict(
		team_id INTEGER NOT NULL DEFAULT nextval('team_id_seq'),
		text VARCHAR (50) NOT NULL,
		category_id INTEGER NOT NULL,
		PRIMARY KEY (text, category_id)
		);
ALTER TABLE team_dict
ADD CONSTRAINT unique_text_category UNIQUE (text, category_id);




CREATE TABLE markets(
		market_id SERIAL PRIMARY KEY NOT NULL,
		event_id UUID NOT NULL,
		bookmaker_key VARCHAR(50) NOT NULL,
		bet_type_id INTEGER NOT NULL,
		market_bookmaker_id VARCHAR(50),
		description VARCHAR (100),
		last_update TIMESTAMP NOT NULL,
		FOREIGN KEY (bet_type_id) REFERENCES bet_types(bet_type_id)
		);


CREATE TABLE outcomes(
		outcome_id SERIAL PRIMARY KEY NOT NULL,
        name VARCHAR (50) NOT NULL,
		description VARCHAR (50) NOT NULL,
		market_id INTEGER NOT NULL,
		bookmaker_outcome_id VARCHAR(50),
        price REAL,
		point REAL
		);

CREATE TABLE sharp_bookmakers(
		bookmaker_key VARCHAR(50) NOT NULL,
)

CREATE TABLE category_groups(
		category_group_id SERIAL PRIMARY KEY NOT NULL,
		name VARCHAR(50) NOT NULL
		);

CREATE TABLE categories(
		category_id SERIAL PRIMARY KEY NOT NULL,
		category_group_id INTEGER NOT NULL REFERENCES category_groups(category_group_id),
		category_name VARCHAR(100) NOT NULL
		);

CREATE TABLE users(
		user_id SERIAL PRIMARY KEY NOT NULL,
		user_name VARCHAR(50) UNIQUE,
		api_key VARCHAR (100),
		telegram_chat_id VARCHAR (100),
		is_admin BOOLEAN
		);      

CREATE TABLE user_bookmakers(
		bookmaker_key VARCHAR(50) NOT NULL,
		user_id INTEGER NOT NULL REFERENCES users(user_id),		
		PRIMARY KEY (bookmaker_key, user_id)
		);

CREATE TABLE user_bets(
		user_id INTEGER NOT NULL REFERENCES users(user_id), 
		outcome_id INTEGER NOT NULL REFERENCES outcomes(outcome_id),
);

CREATE TABLE user_category_groups(
		category_group_id INTEGER NOT NULL REFERENCES category_groups(category_group_id),
		user_id INTEGER NOT NULL REFERENCES users(user_id),		
		PRIMARY KEY (category_group_id, user_id)
		);

CREATE TABLE user_categories(
		category_id INTEGER NOT NULL REFERENCES categories(category_id),
		user_id INTEGER NOT NULL REFERENCES users(user_id),		
		PRIMARY KEY (category_id, user_id)
		);


CREATE TABLE current_odds_anomaly(
		outcome_id INTEGER NOT NULL REFERENCES outcomes(outcome_id),
		anomaly FLOAT	NOT NULL,
		PRIMARY KEY (outcome_id, anomaly)	
		);   


CREATE VIEW readable_events AS
SELECT 
    e.event_id,
    e.commence_time,
    home_team.text AS home,
    away_team.text AS away,
    c.category_name AS category
FROM events e
JOIN team_dict home_team ON e.home = home_team.team_id
JOIN team_dict away_team ON e.away = away_team.team_id
JOIN bookmaker_categories bc ON e.category_id = bc.category_id
JOIN categories c ON bc.category_id = c.category_id;