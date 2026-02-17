
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS datasets CASCADE;


CREATE TABLE datasets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_name VARCHAR(255) NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE NOT NULL,
    version INTEGER DEFAULT 1 NOT NULL
);

CREATE INDEX idx_datasets_name ON datasets(dataset_name);

CREATE INDEX idx_datasets_name_active 
ON datasets(dataset_name, is_deleted) 
WHERE is_deleted = FALSE;


CREATE INDEX idx_datasets_data_gin ON datasets USING GIN (data);


CREATE INDEX idx_datasets_data_jsonb_path ON datasets USING GIN (data jsonb_path_ops);

-- Index on created_at
CREATE INDEX idx_datasets_created_at ON datasets(created_at DESC);

-- Partial index for recent active datasets
CREATE INDEX idx_datasets_recent_active 
ON datasets(created_at DESC) 
WHERE is_deleted = FALSE;


CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_datasets_updated_at
    BEFORE UPDATE ON datasets
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();


COMMENT ON TABLE datasets IS 'Stores JSON datasets with JSONB for efficient querying';
COMMENT ON COLUMN datasets.id IS 'Unique identifier (UUID)';
COMMENT ON COLUMN datasets.dataset_name IS 'Name/identifier for the dataset';
COMMENT ON COLUMN datasets.data IS 'JSON data stored as JSONB';
COMMENT ON COLUMN datasets.created_at IS 'Timestamp when created';
COMMENT ON COLUMN datasets.updated_at IS 'Timestamp when updated';
COMMENT ON COLUMN datasets.is_deleted IS 'Soft delete flag';
COMMENT ON COLUMN datasets.version IS 'Version for optimistic locking';