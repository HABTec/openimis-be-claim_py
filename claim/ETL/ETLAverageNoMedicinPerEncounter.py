import random , string
from sqlalchemy import text
from opensearchpy import helpers

from core.ETLBase import ETLBase

class AverageMedicinePerEncounterETL(ETLBase):
    INDEX_NAME = "average_medicine_per_encounter"

    def process(self):

        offset = 1000
        rows = self._extract(offset)

        transformed = self.transform(rows)
        self.clear_index(self.INDEX_NAME)
        self.load(transformed)


    def _extract(self, offset: int):
        sql = text("""
            WITH medicines_per_encounter AS (
                SELECT
                    l."LocationId",
                    l."LocationName",
                    c."ClaimID" AS encounter_id,
                    c."ValidityFrom" as date,
                    COUNT(ci."ClaimItemID") AS medicines_count
                FROM "tblClaim" c
                JOIN "tblClaimItems" ci
                    ON ci."ClaimID" = c."ClaimID"
                AND ci."ValidityTo" IS NULL
                JOIN "tblItems" i
                    ON i."ItemID" = ci."ItemID"
                AND i."ValidityTo" IS NULL
                AND i."ItemType" = 'D' 
                JOIN "tblHF" hf
                    ON hf."HfID" = c."HFID"
                AND hf."ValidityTo" IS NULL
                JOIN "tblLocations" l
                    ON l."LocationId" = hf."LocationId"
                WHERE c."ValidityTo" IS NULL
                GROUP BY
                    l."LocationId",
                    l."LocationName",
                    c."ClaimID"
            )
            SELECT
                "LocationId",
                "LocationName",
                EXTRACT(MONTH FROM to_ethiopian_date("date"::DATE)::date) AS month ,
                EXTRACT(YEAR FROM to_ethiopian_date("date"::DATE)::date) AS year ,
                COUNT(encounter_id) AS total_encounters,
                SUM(medicines_count) AS total_medicines_prescribed,
                ROUND(
                    SUM(medicines_count)::numeric
                    / NULLIF(COUNT(encounter_id), 0),
                    2
                ) AS avg_medicines_per_encounter
            FROM medicines_per_encounter
            GROUP BY
                "LocationId",
                "LocationName",
                "date"
            ORDER BY
                avg_medicines_per_encounter DESC;


        """)
        conn = self.engine.connect()
        result = conn.execute(
            sql,
            # {
            #     "limit": self.BATCH_SIZE,
            #     "offset": offset,
            # }
        )
        res = result.mappings().all()
        return res
     
    def transform(self, rows):
        docs = []

        for row in rows:
            docs.append({
                "_index": self.INDEX_NAME,
                "_source": {
                    "location_id": row.LocationId,
                    "location_name": row.LocationName,
                    "total_encounters": row.total_encounters,
                    "total_medicines_prescribed": row.total_medicines_prescribed,
                    "avg_medicines_per_encounter": float(row.avg_medicines_per_encounter),
                    "month": int(row.month),
                    "year": int(row.year),
                }
            })

        return docs

    def load(self, docs):
        if not docs:
            return

        success, failed = helpers.bulk(
            client=self.opensearch,
            actions=docs,
            stats_only=True,
        )

        print("Success:", success)
        print("Failed:", failed)
