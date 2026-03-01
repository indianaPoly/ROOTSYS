CREATE TABLE IF NOT EXISTS defect_events (
  defect_id TEXT NOT NULL,
  lot_id TEXT NOT NULL,
  notes TEXT NOT NULL
);

TRUNCATE defect_events;

INSERT INTO defect_events (defect_id, lot_id, notes)
VALUES
  ('PG-1001', 'LOT-PG-1', 'postgres smoke row 1'),
  ('PG-1002', 'LOT-PG-2', 'postgres smoke row 2');
