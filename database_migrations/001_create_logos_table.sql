-- =============================================================================
-- Migration: 001_create_logos_table.sql
-- Module: Core Brand / Employee Portal
-- Schema: CHAKORA / SUPPORT
-- Target DB: Oracle 23ai / 19c
-- Standards: SDLC Compliance, Enterprise Audit Columns & Primary Key Constraints
-- =============================================================================

-- Table: LOGOS
-- Description: Stores centralized brand assets and S3 URLs for portal components
CREATE TABLE LOGOS (
    SNO          NUMBER PRIMARY KEY,
    NAME         VARCHAR2(100) NOT NULL,
    AWS_S3_URL   VARCHAR2(1000) NOT NULL,
    ALT_TEXT     VARCHAR2(255) DEFAULT 'ChakoraHub Official Logo',
    IS_ACTIVE    NUMBER(1) DEFAULT 1 NOT NULL,
    CREATED_AT   TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    UPDATED_AT   TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Comments for Data Dictionary
COMMENT ON TABLE LOGOS IS 'Centralized repository for brand assets and CDN/S3 image URLs';
COMMENT ON COLUMN LOGOS.SNO IS 'Surrogate primary key identifying the logo record';
COMMENT ON COLUMN LOGOS.NAME IS 'Unique identifier or brand name for the logo';
COMMENT ON COLUMN LOGOS.AWS_S3_URL IS 'Full S3/CloudFront HTTPS endpoint for the logo asset';
COMMENT ON COLUMN LOGOS.IS_ACTIVE IS 'Flag (1=Active, 0=Inactive) for soft deletion';

-- Initial Seed Data
INSERT INTO LOGOS (SNO, NAME, AWS_S3_URL, ALT_TEXT, IS_ACTIVE, CREATED_AT, UPDATED_AT)
VALUES (
    1,
    'ChakoraHub',
    'https://d1pjjckqswt5z7.cloudfront.net/logo.png',
    'ChakoraHub Official Logo',
    1,
    SYSTIMESTAMP,
    SYSTIMESTAMP
);

COMMIT;
