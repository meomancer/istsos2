createsqlschema = """

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;
SET default_tablespace = '';
SET default_with_oids = false;
SET TimeZone='0';

--=====================================
CREATE TABLE event_time (
    id_eti bigint NOT NULL,
    id_prc_fk integer NOT NULL,
    time_eti timestamp with time zone NOT NULL
);
COMMENT ON TABLE event_time IS 'Stores Observation''s eventTime.';

CREATE SEQUENCE event_time_id_eti_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE event_time_id_eti_seq OWNED BY event_time.id_eti;
--=====================================
CREATE TABLE feature_type (
    name_fty character varying(25) NOT NULL,
    id_fty integer NOT NULL
);
COMMENT ON TABLE feature_type IS 'Definition of FeatureOfInterest type.';

CREATE SEQUENCE feature_type_id_fty_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE feature_type_id_fty_seq OWNED BY feature_type.id_fty;
--=====================================
CREATE TABLE foi (
    desc_foi text,
    id_fty_fk integer NOT NULL,
    id_foi integer NOT NULL,
    name_foi character varying(25) NOT NULL
);
SELECT AddGeometryColumn('foi', 'geom_foi', $SRID, 'POINT', 3);
COMMENT ON TABLE foi IS 'Stores FeatureOfInterest type.';

CREATE SEQUENCE foi_id_foi_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE foi_id_foi_seq OWNED BY foi.id_foi;


--=====================================

--=====================================
CREATE TABLE measures (
    id_msr bigint NOT NULL,
    id_eti_fk bigint NOT NULL,
    id_qi_fk integer NOT NULL,
    id_pro_fk integer NOT NULL,
    val_msr numeric(10,6) NOT NULL
);
COMMENT ON TABLE measures IS 'Stores the measures of the Procedure.';

CREATE SEQUENCE measures_id_msr_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE measures_id_msr_seq OWNED BY measures.id_msr;
--=====================================
CREATE TABLE positions (
    id_pos bigint NOT NULL,
    id_qi_fk integer NOT NULL,
    id_eti_fk bigint NOT NULL
);
SELECT AddGeometryColumn('positions', 'geom_pos', $SRID, 'POINT', 3);
COMMENT ON TABLE positions IS 'Stores the location for mobile-points Procedure.';

CREATE SEQUENCE measures_mobile_id_mmo_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE measures_mobile_id_mmo_seq OWNED BY positions.id_pos;
--=====================================
CREATE TABLE observed_properties (
    name_opr character varying(60) NOT NULL,
    def_opr character varying(80) NOT NULL,
    desc_opr text,
    constr_opr character varying,
    id_opr integer NOT NULL
);
COMMENT ON TABLE observed_properties IS 'Stores the ObservedProperties.';

CREATE SEQUENCE obs_pr_id_opr_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE obs_pr_id_opr_seq OWNED BY observed_properties.id_opr;
--=====================================
CREATE TABLE obs_type (
    id_oty integer NOT NULL,
    name_oty character varying(60) NOT NULL,
    desc_oty character varying(120)
);
COMMENT ON TABLE obs_type IS 'Stores the type of observation (e.g.: mobile or fix).';

CREATE SEQUENCE obs_type_id_oty_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE obs_type_id_oty_seq OWNED BY obs_type.id_oty;
--=====================================
CREATE TABLE off_proc (
    id_off_prc integer NOT NULL,
    id_off_fk integer NOT NULL,
    id_prc_fk integer NOT NULL
);
COMMENT ON TABLE off_proc IS 'Association table between Offerings and Procedures.';

CREATE SEQUENCE off_proc_id_opr_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE off_proc_id_opr_seq OWNED BY off_proc.id_off_prc;
--=====================================
CREATE TABLE offerings (
    name_off character varying(64) NOT NULL,
    desc_off text,
    expiration_off timestamp with time zone,
    active_off boolean DEFAULT true NOT NULL,
    id_off integer NOT NULL
);
COMMENT ON TABLE offerings IS 'Stores the Offerings.';

CREATE SEQUENCE offerings_id_off_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE offerings_id_off_seq OWNED BY offerings.id_off;
--=====================================
CREATE TABLE proc_obs (
    id_pro integer NOT NULL,
    id_prc_fk integer NOT NULL,
    id_uom_fk integer NOT NULL,
    id_opr_fk integer NOT NULL,
    constr_pro character varying
);
COMMENT ON TABLE proc_obs IS 'Association table between Procedures, ObservedProperty and UnitOfMeasure.';

CREATE SEQUENCE prc_obs_id_pro_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE prc_obs_id_pro_seq OWNED BY proc_obs.id_pro;
--=====================================
CREATE TABLE procedures (
    name_prc character varying(30) NOT NULL,
    desc_prc text,
    stime_prc timestamp with time zone,
    etime_prc timestamp with time zone,
    id_prc integer NOT NULL,
    id_tru_fk integer NOT NULL,
    time_res_prc integer,
    id_oty_fk integer,
    id_foi_fk integer,
    assignedid_prc character varying(32) NOT NULL
);
COMMENT ON TABLE procedures IS 'Stores the Procedures.';

CREATE SEQUENCE procedures_id_prc_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE procedures_id_prc_seq OWNED BY procedures.id_prc;
--=====================================
CREATE TABLE quality_index (
    name_qi character varying(25) NOT NULL,
    desc_qi text,
    id_qi integer NOT NULL
);
COMMENT ON TABLE quality_index IS 'Stores the QualityIndexes.';
--=====================================
CREATE TABLE time_res_unit (
    id_tru integer NOT NULL,
    name_tru character varying(15)
);
COMMENT ON TABLE time_res_unit IS 'Stores the Procedure''s time resolution units.';

CREATE SEQUENCE time_res_unit_id_tru_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE time_res_unit_id_tru_seq OWNED BY time_res_unit.id_tru;
--=====================================
CREATE TABLE uoms (
    name_uom character varying(20) NOT NULL,
    desc_uom text,
    id_uom integer NOT NULL
);
COMMENT ON TABLE uoms IS 'Stores the Units of Measures.';

CREATE SEQUENCE uoms_id_uom_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE uoms_id_uom_seq OWNED BY uoms.id_uom;
--=====================================
CREATE TABLE tran_log (
    id_trl integer NOT NULL,
    operation_trl character varying NOT NULL,
    procedure_trl character varying(30) NOT NULL,
    begin_trl timestamp with time zone,
    end_trl timestamp with time zone,
    count integer,
    stime_prc timestamp with time zone,
    etime_prc timestamp with time zone
);
COMMENT ON TABLE tran_log IS 'Log table for transactional operations.';

CREATE SEQUENCE tran_log_id_trl_seq
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;
ALTER SEQUENCE tran_log_id_trl_seq OWNED BY tran_log.id_trl;
--=====================================
-- NEXTVALS
--=====================================

ALTER TABLE event_time ALTER COLUMN id_eti SET DEFAULT nextval('event_time_id_eti_seq'::regclass);
ALTER TABLE feature_type ALTER COLUMN id_fty SET DEFAULT nextval('feature_type_id_fty_seq'::regclass);
ALTER TABLE foi ALTER COLUMN id_foi SET DEFAULT nextval('foi_id_foi_seq'::regclass);
ALTER TABLE measures ALTER COLUMN id_msr SET DEFAULT nextval('measures_id_msr_seq'::regclass);
ALTER TABLE obs_type ALTER COLUMN id_oty SET DEFAULT nextval('obs_type_id_oty_seq'::regclass);
ALTER TABLE observed_properties ALTER COLUMN id_opr SET DEFAULT nextval('obs_pr_id_opr_seq'::regclass);
ALTER TABLE off_proc ALTER COLUMN id_off_prc SET DEFAULT nextval('off_proc_id_opr_seq'::regclass);
ALTER TABLE offerings ALTER COLUMN id_off SET DEFAULT nextval('offerings_id_off_seq'::regclass);
ALTER TABLE positions ALTER COLUMN id_pos SET DEFAULT nextval('measures_mobile_id_mmo_seq'::regclass);
ALTER TABLE proc_obs ALTER COLUMN id_pro SET DEFAULT nextval('prc_obs_id_pro_seq'::regclass);
ALTER TABLE procedures ALTER COLUMN id_prc SET DEFAULT nextval('procedures_id_prc_seq'::regclass);
ALTER TABLE time_res_unit ALTER COLUMN id_tru SET DEFAULT nextval('time_res_unit_id_tru_seq'::regclass);
ALTER TABLE uoms ALTER COLUMN id_uom SET DEFAULT nextval('uoms_id_uom_seq'::regclass);
ALTER TABLE tran_log ALTER COLUMN id_trl SET DEFAULT nextval('tran_log_id_trl_seq'::regclass);

--=====================================
-- CONSTRAINTS
--=====================================
ALTER TABLE ONLY event_time
    ADD CONSTRAINT event_time_id_prc_fk_key UNIQUE (id_prc_fk, time_eti);
ALTER TABLE ONLY event_time
    ADD CONSTRAINT event_time_pkey PRIMARY KEY (id_eti);
ALTER TABLE ONLY feature_type
    ADD CONSTRAINT feature_type_pkey PRIMARY KEY (id_fty);
ALTER TABLE ONLY foi
    ADD CONSTRAINT foi_name_foi_key UNIQUE (name_foi);
ALTER TABLE ONLY foi
    ADD CONSTRAINT foi_pkey PRIMARY KEY (id_foi);
ALTER TABLE ONLY measures
    ADD CONSTRAINT measures_fix_pkey PRIMARY KEY (id_msr);
ALTER TABLE ONLY measures
    ADD CONSTRAINT measures_id_eti_fk_key UNIQUE (id_eti_fk, id_pro_fk);
ALTER TABLE ONLY positions
    ADD CONSTRAINT measures_mobile_pkey PRIMARY KEY (id_pos);
ALTER TABLE ONLY observed_properties
    ADD CONSTRAINT obs_pr_name_opr_key UNIQUE (name_opr);
ALTER TABLE ONLY observed_properties
    ADD CONSTRAINT obs_pr_pkey PRIMARY KEY (id_opr);
ALTER TABLE ONLY obs_type
    ADD CONSTRAINT obs_type_pkey PRIMARY KEY (id_oty);
ALTER TABLE ONLY off_proc
    ADD CONSTRAINT off_proc_id_off_fk_key UNIQUE (id_off_fk, id_prc_fk);
ALTER TABLE ONLY off_proc
    ADD CONSTRAINT off_proc_pkey PRIMARY KEY (id_off_prc);
ALTER TABLE ONLY offerings
    ADD CONSTRAINT offerings_name_off_key UNIQUE (name_off);
ALTER TABLE ONLY offerings
    ADD CONSTRAINT offerings_pkey PRIMARY KEY (id_off);
ALTER TABLE ONLY proc_obs
    ADD CONSTRAINT prc_obs_pkey PRIMARY KEY (id_pro);
ALTER TABLE ONLY proc_obs
    ADD CONSTRAINT proc_obs_id_uom_fk_key UNIQUE (id_uom_fk, id_opr_fk, id_prc_fk);
ALTER TABLE ONLY procedures
    ADD CONSTRAINT procedures_assignedid_prc_key UNIQUE (assignedid_prc);
ALTER TABLE ONLY procedures
    ADD CONSTRAINT procedures_pkey PRIMARY KEY (id_prc);
ALTER TABLE ONLY quality_index
    ADD CONSTRAINT quality_index_pkey PRIMARY KEY (id_qi);
ALTER TABLE ONLY time_res_unit
    ADD CONSTRAINT time_res_unit_pkey PRIMARY KEY (id_tru);
ALTER TABLE ONLY uoms
    ADD CONSTRAINT uoms_pkey PRIMARY KEY (id_uom);
ALTER TABLE ONLY event_time
    ADD CONSTRAINT event_time_id_prc_fk_fkey FOREIGN KEY (id_prc_fk) REFERENCES procedures(id_prc) ON DELETE CASCADE;
ALTER TABLE ONLY foi
    ADD CONSTRAINT foi_id_fty_fk_fkey FOREIGN KEY (id_fty_fk) REFERENCES feature_type(id_fty);
ALTER TABLE ONLY measures
    ADD CONSTRAINT measures_fix_id_qi_fk_fkey FOREIGN KEY (id_qi_fk) REFERENCES quality_index(id_qi) ON UPDATE CASCADE;
ALTER TABLE ONLY measures
    ADD CONSTRAINT measures_id_eti_fk_fkey FOREIGN KEY (id_eti_fk) REFERENCES event_time(id_eti) ON DELETE CASCADE;
ALTER TABLE ONLY measures
    ADD CONSTRAINT measures_id_pro_fk_fkey FOREIGN KEY (id_pro_fk) REFERENCES proc_obs(id_pro);
ALTER TABLE ONLY positions
    ADD CONSTRAINT measures_mobile_id_qi_fk_fkey FOREIGN KEY (id_qi_fk) REFERENCES quality_index(id_qi) ON UPDATE CASCADE;
ALTER TABLE ONLY off_proc
    ADD CONSTRAINT off_proc_id_off_fk_fkey FOREIGN KEY (id_off_fk) REFERENCES offerings(id_off) ON DELETE CASCADE;
ALTER TABLE ONLY off_proc
    ADD CONSTRAINT off_proc_id_prc_fk_fkey FOREIGN KEY (id_prc_fk) REFERENCES procedures(id_prc) ON DELETE CASCADE;
ALTER TABLE ONLY positions
    ADD CONSTRAINT positions_id_eti_fk_fkey FOREIGN KEY (id_eti_fk) REFERENCES event_time(id_eti) ON DELETE CASCADE;
ALTER TABLE ONLY proc_obs
    ADD CONSTRAINT prc_obs_id_opr_fk_fkey FOREIGN KEY (id_opr_fk) REFERENCES observed_properties(id_opr);
ALTER TABLE ONLY proc_obs
    ADD CONSTRAINT prc_obs_id_uom_fk_fkey FOREIGN KEY (id_uom_fk) REFERENCES uoms(id_uom);
ALTER TABLE ONLY proc_obs
    ADD CONSTRAINT proc_obs_id_prc_fk_fkey FOREIGN KEY (id_prc_fk) REFERENCES procedures(id_prc) ON DELETE CASCADE;
ALTER TABLE ONLY procedures
    ADD CONSTRAINT procedures_id_foi_fk_fkey FOREIGN KEY (id_foi_fk) REFERENCES foi(id_foi);
ALTER TABLE ONLY procedures
    ADD CONSTRAINT procedures_id_oty_fk_fkey FOREIGN KEY (id_oty_fk) REFERENCES obs_type(id_oty);
ALTER TABLE ONLY procedures
    ADD CONSTRAINT procedures_id_tru_fk_fkey FOREIGN KEY (id_tru_fk) REFERENCES time_res_unit(id_tru);
ALTER TABLE ONLY tran_log
    ADD CONSTRAINT tran_log_pkey PRIMARY KEY (id_trl);
   
--=====================================
-- INDEXES
--=====================================
CREATE INDEX ety_prc_date ON event_time USING btree (id_eti, time_eti);

--=====================================
-- CONSTANT/DEFAULT VALUES
--=====================================
INSERT INTO quality_index (name_qi, desc_qi, id_qi) VALUES ('aggregation no data', 'no values are present for this aggregation interval', -100);
INSERT INTO quality_index (name_qi, desc_qi, id_qi) VALUES ('outboud', 'gross error', 0);
INSERT INTO quality_index (name_qi, desc_qi, id_qi) VALUES ('raw', 'the format is correct', 100);
INSERT INTO quality_index (name_qi, desc_qi, id_qi) VALUES ('acceptable', 'the value is acceptable for the observed property', 110);
INSERT INTO quality_index (name_qi, desc_qi, id_qi) VALUES ('reasonable', 'the value is in a resonable range', 200);
INSERT INTO quality_index (name_qi, desc_qi, id_qi) VALUES ('timely coherent', 'the value is coherent with time-series', 300);
INSERT INTO quality_index (name_qi, desc_qi, id_qi) VALUES ('spatilly coherent', 'the value is coherent with close by observations', 400);
INSERT INTO quality_index (name_qi, desc_qi, id_qi) VALUES ('manually adjusted', 'the value has been manually corrected', 500);
INSERT INTO quality_index (name_qi, desc_qi, id_qi) VALUES ('correct', 'the value has not been modified and is correct', 600);


INSERT INTO obs_type (id_oty, name_oty, desc_oty) VALUES (1, 'insitu-fixed-point', 'fixed, in-situ, pointwise observation');
INSERT INTO obs_type (id_oty, name_oty, desc_oty) VALUES (2, 'insitu-mobile-point', 'mobile, in-situ, pointwise observation');
INSERT INTO obs_type (id_oty, name_oty, desc_oty) VALUES (3, 'virtual', 'virtual procedure');




-- Completed on 2010-10-07 16:53:49 CEST

--
-- PostgreSQL database dump complete
--
"""