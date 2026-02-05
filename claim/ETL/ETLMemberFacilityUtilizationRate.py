import random , string
from sqlalchemy import text
from opensearchpy import helpers

from core.ETLBase import ETLBase

class MemberUtilizationRateETL(ETLBase):
    INDEX_NAME = "member_utilization_rate"

    def process(self):

        offset = 1000
        rows = self._extract(offset)

        transformed = self.transform(rows)
        self.clear_index(self.INDEX_NAME)
        self.load(transformed)


    def _extract(self, offset: int):
        sql = text("""
            SELECT
                d."LocationId",
                d.total_beneficiaries_visited,
                b.total_active_beneficiaries,
                EXTRACT(MONTH FROM to_ethiopian_date(d."ValidityFrom"::DATE)::date) AS month ,
                EXTRACT(YEAR FROM to_ethiopian_date(d."ValidityFrom"::DATE)::date) AS year ,
                ROUND(
                    (d.total_beneficiaries_visited::numeric
                    / NULLIF(b.total_active_beneficiaries, 0)) * 100,
                    2
                ) AS cbhi_utilization_percentage
            FROM
            (
                SELECT
                    f."LocationId",
                    c."ValidityFrom",
                    COUNT(DISTINCT c."InsureeID") AS total_beneficiaries_visited
                FROM "tblClaim" c
                JOIN "tblInsuree" i
                    ON i."InsureeID" = c."InsureeID"
                AND i."ValidityTo" IS NULL
                AND i."status" = 'AC'
                JOIN "tblFamilies" f
                    ON f."FamilyID" = i."FamilyID"
                AND f."ValidityTo" IS NULL
                WHERE c."ValidityTo" IS NULL
            --      AND c."DateFrom" BETWEEN DATE '2020-01-01' AND DATE '2025-12-31'
                GROUP BY f."LocationId",c."ValidityFrom"
            ) d
            JOIN
            (
                SELECT
                    f."LocationId",
                    COUNT(i."InsureeID") AS total_active_beneficiaries
                FROM "tblPolicy" p
                JOIN "tblFamilies" f
                    ON f."FamilyID" = p."FamilyID"
                AND f."ValidityTo" IS NULL
                JOIN "tblInsuree" i
                    ON i."FamilyID" = f."FamilyID"
                AND i."ValidityTo" IS NULL
                AND i."status" = 'AC'
                WHERE p."ValidityTo" IS null 
                AND p."PolicyStatus" = 2
                GROUP BY f."LocationId"
            ) b
                ON b."LocationId" = d."LocationId"
            ORDER BY d."LocationId";

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
                    "total_beneficiaries_visited": row.total_beneficiaries_visited,
                    "total_active_beneficiaries": row.total_active_beneficiaries,
                    "cbhi_utilization_percentage": row.cbhi_utilization_percentage,
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
