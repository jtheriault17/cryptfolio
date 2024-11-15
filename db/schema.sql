-- DROP SCHEMA IF EXISTS coins CASCADE;
-- DROP SCHEMA IF EXISTS f8949 CASCADE;
-- DROP SCHEMA IF EXISTS historical_data CASCADE;
-- DROP SCHEMA IF EXISTS portfolio CASCADE;
-- DROP SCHEMA IF EXISTS sold CASCADE;
-- DROP SCHEMA IF EXISTS transactions CASCADE;
-- DROP SCHEMA IF EXISTS market CASCADE;

CREATE SCHEMA IF NOT EXISTS market;
CREATE SCHEMA IF NOT EXISTS coins;
CREATE SCHEMA IF NOT EXISTS f8949;
CREATE SCHEMA IF NOT EXISTS historical_data;
CREATE SCHEMA IF NOT EXISTS portfolio;
CREATE SCHEMA IF NOT EXISTS sold;
CREATE SCHEMA IF NOT EXISTS transactions;

--coins

CREATE TABLE IF NOT EXISTS coins.coin_data
(
    coin character varying(80) COLLATE pg_catalog."default" PRIMARY KEY,
    price real,
    quantity_bought real,
    quantity_sold real,
    current_quantity real,
    cost_basis_bought real,
    cost_basis_sold real,
    current_cost_basis real,
    value real,
    unrealized_return real,
    realized_return real
);


CREATE TABLE IF NOT EXISTS coins.coin_list (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS coins.user_coins
(
    symbol character varying(80) COLLATE pg_catalog."default" PRIMARY KEY,
    id character varying(80) COLLATE pg_catalog."default"
);


--portfolio

CREATE TABLE IF NOT EXISTS portfolio.cost_basis
(
    date date,
    cost_basis real,
    CONSTRAINT unique_date_cost_basis UNIQUE (date)
);

CREATE TABLE IF NOT EXISTS portfolio.portfolio
(
    date date,
    coin character varying(80) COLLATE pg_catalog."default",
    quantity real,
    value real,
    cost_basis real,
    UNIQUE (date, coin)
);

CREATE TABLE IF NOT EXISTS portfolio.value
(
    date date,
    value real,
    CONSTRAINT unique_date_value UNIQUE (date)
);


--transactions

CREATE TABLE IF NOT EXISTS transactions.sell
(
    currency character varying(80) COLLATE pg_catalog."default",
    quantity real,
    date_acquired timestamp without time zone,
    date_sold timestamp without time zone,
    proceeds real,
    cost_basis real,
    "return" real,
    term character varying(80) COLLATE pg_catalog."default"
);

CREATE TABLE IF NOT EXISTS transactions.tax_loss_harvesting
(
    quantity_sold real,
    currency character varying(80) COLLATE pg_catalog."default",
    date_acquired timestamp without time zone,
    date_sold timestamp without time zone,
    proceeds real,
    cost_basis real,
    loss real
);


CREATE TABLE IF NOT EXISTS transactions.transactions
(
    date timestamp without time zone PRIMARY KEY,
    type character varying(80) COLLATE pg_catalog."default",
    received_quantity real,
    received_currency character varying(80) COLLATE pg_catalog."default",
    received_cost_basis real,
    sent_quantity real,
    sent_currency character varying(80) COLLATE pg_catalog."default",
    sent_cost_basis real,
    fee_amount real,
    fee_currency character varying(80) COLLATE pg_catalog."default",
    fee_cost_basis real,
    realized_return real,
    fee_realized_return real
);

CREATE TABLE IF NOT EXISTS transactions.transactions_after_sales
(
    date timestamp with time zone PRIMARY KEY,
    currency character varying(80) COLLATE pg_catalog."default",
    price real,
    quantity real,
    cost_basis real
);

--market

CREATE TABLE IF NOT EXISTS market.data
(
    id character varying(50) COLLATE pg_catalog."default" PRIMARY KEY,
    symbol character varying(10) COLLATE pg_catalog."default",
    name character varying(100) COLLATE pg_catalog."default",
    image character varying(255) COLLATE pg_catalog."default",
    current_price real,
    market_cap bigint,
    market_cap_rank integer,
    fully_diluted_valuation bigint,
    total_volume bigint,
    high_24h real,
    low_24h real,
    price_change_24h real,
    price_change_percentage_24h real,
    market_cap_change_24h real,
    market_cap_change_percentage_24h real,
    circulating_supply real,
    total_supply real,
    max_supply real,
    ath real,
    ath_change_percentage real,
    ath_date timestamp with time zone,
    atl real,
    atl_change_percentage real,
    atl_date timestamp with time zone,
    roi real,
    last_updated timestamp with time zone,
    price_change_percentage_14d_in_currency real,
    price_change_percentage_1h_in_currency real,
    price_change_percentage_1y_in_currency real,
    price_change_percentage_200d_in_currency real,
    price_change_percentage_24h_in_currency real,
    price_change_percentage_30d_in_currency real,
    price_change_percentage_7d_in_currency real
)