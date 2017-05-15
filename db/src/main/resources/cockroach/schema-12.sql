CREATE TABLE record (
    id BYTES NOT NULL,
    typeid BYTES NOT NULL,
    data BYTES NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (typeid ASC, id ASC),
    FAMILY "primary" (id, typeid, data)
)

CREATE INDEX k_record_id ON record (id);

CREATE TABLE recordlocation3 (
    id BYTES NOT NULL,
    typeid BYTES NOT NULL,
    symbolid INT NOT NULL,
    value FLOAT NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (symbolid ASC, value ASC, typeid ASC, id ASC),
    FAMILY "primary" (id, typeid, symbolid, value)
)

CREATE INDEX k_recordlocation_id ON recordlocation3 (id);

CREATE TABLE recordnumber3 (
    id BYTES NOT NULL,
    typeid BYTES NOT NULL,
    symbolid INT NOT NULL,
    value INT NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (symbolid ASC, value ASC, typeid ASC, id ASC),
    FAMILY "primary" (id, typeid, symbolid, value)
)

CREATE INDEX k_recordnumber3_id ON recordnumber3 (id);

CREATE TABLE recordregion2 (
    id BYTES NOT NULL,
    typeid BYTES NOT NULL,
    symbolid INT NOT NULL,
    value FLOAT NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (symbolid ASC, value ASC, typeid ASC, id ASC),
    FAMILY "primary" (id, typeid, symbolid, value)
)

CREATE INDEX IF NOT EXISTS k_recordregion2_id ON recordregion2 (id);

CREATE TABLE recordstring4 (
    id BYTES NOT NULL,
    typeid BYTES NOT NULL,
    symbolid INT NOT NULL,
    value BYTES NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (symbolid ASC, value ASC, typeid ASC, id ASC),
    FAMILY "primary" (id, typeid, symbolid, value)
)

CREATE INDEX k_recordstring4_id ON recordstring4 (id);

CREATE TABLE recordupdate (
    id BYTES NOT NULL,
    typeid BYTES NOT NULL,
    updatedate FLOAT NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (id ASC),
    FAMILY "primary" (id, typeid, updatedate)
)

CREATE INDEX k_recordupdate_typeid_updatedate ON recordupdate (typeid, updatedate);

CREATE INDEX k_recordupdate_updatedate ON recordupdate (updatedate);

CREATE TABLE recorduuid3 (
    id BYTES NOT NULL,
    typeid BYTES NOT NULL,
    symbolid INT NOT NULL,
    value BYTES NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (symbolid ASC, value ASC, typeid ASC, id ASC),
    FAMILY "primary" (id, typeid, symbolid, value)
)

CREATE INDEX k_recorduuid3_id ON recorduuid3 (id);

CREATE TABLE symbol (
    symbolid SERIAL NOT NULL,
    value BYTES NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (symbolid ASC),
    FAMILY "primary" (symbolid, value)
)

CREATE UNIQUE INDEX k_symbol_value ON symbol (value);
