import pathlib
import sqlite3

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DB_DIR = BASE_DIR / "tests" / "fixtures" / "db"
DB_DIR.mkdir(parents=True, exist_ok=True)


def create_mes_db(path: pathlib.Path) -> None:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS defect_events (
            defect_id TEXT NOT NULL,
            lot_id TEXT NOT NULL,
            equipment_id TEXT,
            occurred_at TEXT,
            defect_code TEXT,
            notes TEXT
        )
        """
    )
    cur.execute("DELETE FROM defect_events")
    cur.executemany(
        """
        INSERT INTO defect_events
            (defect_id, lot_id, equipment_id, occurred_at, defect_code, notes)
        VALUES
            (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "DEF-1001",
                "LOT-77",
                "EQ-01",
                "2026-02-14T09:15:00Z",
                "SCRATCH",
                "surface scratch on batch",
            ),
            (
                "DEF-1002",
                "LOT-78",
                "EQ-02",
                "2026-02-14T09:45:00Z",
                "CRACK",
                "micro crack detected",
            ),
        ],
    )
    conn.commit()
    conn.close()


def create_qms_db(path: pathlib.Path) -> None:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS quality_claims (
            claim_id TEXT NOT NULL,
            defect_id TEXT,
            severity INTEGER,
            description TEXT,
            created_at TEXT
        )
        """
    )
    cur.execute("DELETE FROM quality_claims")
    cur.executemany(
        """
        INSERT INTO quality_claims
            (claim_id, defect_id, severity, description, created_at)
        VALUES
            (?, ?, ?, ?, ?)
        """,
        [
            (
                "CLAIM-9001",
                "DEF-1001",
                3,
                "customer reported scratch",
                "2026-02-14T10:05:00Z",
            ),
            (
                "CLAIM-9002",
                "DEF-1003",
                2,
                "edge defect",
                "2026-02-14T10:12:00Z",
            ),
        ],
    )
    conn.commit()
    conn.close()


def main() -> None:
    create_mes_db(DB_DIR / "mes.db")
    create_qms_db(DB_DIR / "qms.db")


if __name__ == "__main__":
    main()
