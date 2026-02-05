import random , string
from sqlalchemy import text
from opensearchpy import helpers

from core.ETLBase import ETLBase

class ValueAndCountOfClaimTypesETL(ETLBase):
    INDEX_NAME = "value_and_count_of_claim_types"

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
                hf."HFLevel",
                EXTRACT(MONTH FROM to_ethiopian_date(c."ValidityFrom" ::DATE)::date) AS month ,
                EXTRACT(YEAR FROM to_ethiopian_date(c."ValidityFrom" ::DATE)::date) AS year ,
                COUNT(CASE 
                    WHEN c."ClaimStatus" = 8 
                    AND c."Remunerated" IS NOT NULL 
                    THEN 1 
                END) AS paid_claim_count,
                COALESCE(SUM(CASE 
                    WHEN c."ClaimStatus" = 8 
                    AND c."Remunerated" IS NOT NULL 
                    THEN c."Remunerated" 
                END), 0) AS paid_claim_amount,
                COUNT(CASE 
                    WHEN c."ClaimStatus" IN (16) 
                    AND c."Remunerated" IS NULL 
                    THEN 1 
                END) AS outstanding_claim_count,
                COALESCE(SUM(CASE 
                    WHEN c."ClaimStatus" IN (16) 
                    AND c."Remunerated" IS NULL 
                    THEN c."Approved" 
                END), 0) AS outstanding_claim_amount,
                COUNT(CASE 
                    WHEN c."ClaimStatus" = 1 
                    THEN 1 
                END) AS rejected_claim_count,
                COALESCE(SUM(CASE 
                    WHEN c."ClaimStatus" = 1 
                    THEN c."Claimed" 
                END), 0) AS rejected_claim_amount
            FROM "tblClaim" c
            JOIN "tblHF" hf
                ON hf."HfID" = c."HFID"
            AND hf."ValidityTo" IS NULL
            WHERE c."ValidityTo" IS NULL
            GROUP BY
                hf."LocationId",
                hf."HFLevel",
                c."ValidityFrom" 
            ORDER BY
                hf."LocationId",
                hf."HFLevel";
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
                    "hf_level": row.HFLevel,
                    "paid_claim_count": row.paid_claim_count,
                    "paid_claim_amount": float(row.paid_claim_amount),
                    "outstanding_claim_count": row.outstanding_claim_count, 
                    "outstanding_claim_amount": float(row.outstanding_claim_amount),
                    "rejected_claim_count": row.rejected_claim_count,
                    "rejected_claim_amount": float(row.rejected_claim_amount),
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
