import random , string
from sqlalchemy import text
from opensearchpy import helpers

from core.ETLBase import ETLBase

class NumberOfContractedFacilitiesETL(ETLBase):
    INDEX_NAME = "number_of_contracted_facilities"

    def process(self):

        offset = 1000
        rows = self._extract(offset)

        transformed = self.transform(rows)
        self.clear_index(self.INDEX_NAME)
        self.load(transformed)


    def _extract(self, offset: int):
        sql = text("""
            SELECT
                hfc."LocationId",
                l."LocationName",
                COUNT(DISTINCT hfc."HFID") AS total_contracted_facilities
            FROM "tblHFContract" hfc
            JOIN "tblHF" hf
                ON hf."HfID" = hfc."HFID"
            AND hf."ValidityTo" IS NULL
            JOIN "tblLocations" l
                ON l."LocationId" = hfc."LocationId"
            WHERE 
                hf."ValidityTo" IS null
                and  hfc."EndDate" >= NOW()
            GROUP BY
                hfc."LocationId",
                l."LocationName"
            ORDER BY
                total_contracted_facilities DESC;
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
                    "total_contracted_facilities": row.total_contracted_facilities,
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
