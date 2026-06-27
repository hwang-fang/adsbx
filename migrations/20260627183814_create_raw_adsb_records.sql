-- ADS-B 統合解析・DB登録システム: raw_adsb_records
CREATE TABLE raw_adsb_records (
    timestamp   TIMESTAMP WITH TIME ZONE NOT NULL,
    mode_s_code VARCHAR(6) NOT NULL,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION,
    alt         INT,
    call_sign   VARCHAR(8),
    squawk      VARCHAR(4),
    on_ground   BOOLEAN NOT NULL,
    CONSTRAINT uq_raw_records_modes_time UNIQUE (mode_s_code, timestamp)
);

CREATE INDEX idx_raw_records_time ON raw_adsb_records (timestamp);
CREATE INDEX idx_raw_records_mode_s_time ON raw_adsb_records (mode_s_code, timestamp DESC);
