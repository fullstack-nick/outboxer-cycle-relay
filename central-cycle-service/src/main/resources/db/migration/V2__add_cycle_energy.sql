ALTER TABLE cycle_event
    ADD COLUMN energy_consumption_wh DOUBLE PRECISION NULL
    CHECK (energy_consumption_wh >= 0);
