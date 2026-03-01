CREATE TABLE IF NOT EXISTS defect_events (
  defect_id VARCHAR(64) NOT NULL,
  lot_id VARCHAR(64) NOT NULL,
  notes VARCHAR(255) NOT NULL
);

DELETE FROM defect_events;

INSERT INTO defect_events (defect_id, lot_id, notes)
VALUES
  ('MY-1001', 'LOT-MY-1', 'mysql smoke row 1'),
  ('MY-1002', 'LOT-MY-2', 'mysql smoke row 2');
