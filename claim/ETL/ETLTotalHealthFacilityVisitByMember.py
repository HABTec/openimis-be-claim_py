import random , string
from sqlalchemy import text
from opensearchpy import helpers

from core.ETLBase import ETLBase

class TotalHealthFacilityVisitByMembersETL(ETLBase):
    INDEX_NAME = "total_health_facility_visit_by_member"

    def process(self):

        offset = 1000
        rows = self._extract(offset)

        transformed = self.transform(rows)
        self.clear_index(self.INDEX_NAME)
        self.load(transformed)


    def _extract(self, offset: int):
        sql = text("""
            SELECT
                hf."LocationId",
                COUNT(c."InsureeID") AS total_visits
            FROM "tblClaim" c
            JOIN "tblHF" hf
                ON hf."HfID" = c."HFID"
            AND hf."ValidityTo" IS NULL
            WHERE c."ValidityTo" IS NULL
            AND c."ClaimStatus" IN (2, 4, 8)   -- adjust statuses if needed
            GROUP BY hf."LocationId"
            ORDER BY hf."LocationId";
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
                    "total_visits": row.total_visits,
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
