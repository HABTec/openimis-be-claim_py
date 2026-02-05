import random , string
from sqlalchemy import text
from opensearchpy import helpers

from core.ETLBase import ETLBase

class PatientReferedToAnotherFacilityETL(ETLBase):
    INDEX_NAME = "patient_refered_to_another_facility"

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
                EXTRACT(MONTH FROM to_ethiopian_date(c."ValidityFrom"::DATE)::date) AS month ,
                EXTRACT(YEAR FROM to_ethiopian_date(c."ValidityFrom"::DATE)::date) AS year ,
                COUNT(DISTINCT c."InsureeID") AS total_patients,
                COUNT(DISTINCT CASE
                    WHEN  c."ReferFrom" is not null
                    THEN c."InsureeID"
                END) AS sent_outside_patients,
                ROUND(
                    COUNT(DISTINCT CASE
                        WHEN  c."ReferFrom" is not null
                        THEN c."InsureeID"
                    END)::NUMERIC
                    /
                    NULLIF(COUNT(DISTINCT c."InsureeID"), 0)
                    * 100,
                    2
                ) AS sent_outside_percentage
            FROM "tblClaim" c
            JOIN "tblHF" hf
                ON hf."HfID" = c."HFID"
            AND hf."ValidityTo" IS NULL
            WHERE c."ValidityTo" IS NULL
            and c."DateTo" is not NULL
            GROUP BY
                hf."LocationId",
                c."ValidityFrom" 
            ORDER BY
                hf."LocationId";
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
                    "total_patients": row.total_patients,
                    "sent_outside_patients": row.sent_outside_patients,
                    "sent_outside_percentage": row.sent_outside_percentage,
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
