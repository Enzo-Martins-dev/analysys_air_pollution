CREATE TABLE IF NOT EXISTS qualidade_ar_historico (
    data_hora_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    cidade VARCHAR(100), 

    indice_qualidade_ar INTEGER NOT NULL,
    CHECK (indice_qualidade_ar >= 1 AND indice_qualidade_ar <= 5),

    co_monoxido_carbono DECIMAL(10, 4) NOT NULL,
    no_monoxido_nitrogenio DECIMAL(10, 4) NOT NULL,
    no2_dioxido_nitrogenio DECIMAL(10, 4) NOT NULL,
    o3_ozonio DECIMAL(10, 4) NOT NULL,
    so2_dioxido_enxofre DECIMAL(10, 4) NOT NULL,
    pm2_5_particulado DECIMAL(10, 4) NOT NULL,
    pm10_particulado DECIMAL(10, 4) NOT NULL,
    nh3_amonia DECIMAL(10, 4) NOT NULL,

	PRIMARY KEY (data_hora_utc, latitude, longitude)
);