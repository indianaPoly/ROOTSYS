import argparse
import pathlib
import sqlite3

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DB_DIR = BASE_DIR / "tests" / "fixtures" / "db"
DB_DIR.mkdir(parents=True, exist_ok=True)


def create_mes_db(path: pathlib.Path, count: int) -> None:
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
                f"DEF-{1000 + idx}",
                f"LOT-{76 + idx}",
                f"EQ-{(idx % 12) + 1:02d}",
                f"2026-02-14T09:{(idx % 60):02d}:00Z",
                "CRACK" if idx % 2 == 0 else "SCRATCH",
                f"generated mes row {idx}",
            )
            for idx in range(1, count + 1)
        ],
    )
    conn.commit()
    conn.close()


def create_qms_db(path: pathlib.Path, count: int) -> None:
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
                f"CLAIM-{9000 + idx}",
                f"DEF-{1000 + ((idx - 1) % max(1, count)) + 1}",
                (idx % 5) + 1,
                f"generated qms row {idx}",
                f"2026-02-14T10:{(idx % 60):02d}:00Z",
            )
            for idx in range(1, count + 1)
        ],
    )
    conn.commit()
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mes-count", type=int, default=2)
    parser.add_argument("--qms-count", type=int, default=2)
    args = parser.parse_args()

    if args.mes_count <= 0 or args.qms_count <= 0:
        raise SystemExit("--mes-count and --qms-count must be > 0")

    create_mes_db(DB_DIR / "mes.db", args.mes_count)
    create_qms_db(DB_DIR / "qms.db", args.qms_count)


if __name__ == "__main__":
    main()
