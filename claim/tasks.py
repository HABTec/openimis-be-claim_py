import logging

from insuree.ETL import ETLEnrollmentRate


logger = logging.getLogger(__name__)


from claim.ETL import ETLAverageNoMedicinPerEncounter,ETLBillablesWithFrequencyAndCost, ETLClaimToPremiumRatio, ETLMemberFacilityUtilizationRate, ETLNumberOfContractedFacilities, ETLPatientReferedToAnotherFacility,ETLPatientSatisfactionRate,ETLTotalHealthFacilityVisitByMember,ETLValueAndCountOfClaimTypes
def Claim_ETL():
    averageNoMedicinPerEncounterETL = ETLAverageNoMedicinPerEncounter.AverageMedicinePerEncounterETL()
    averageNoMedicinPerEncounterETL.process()
    billablesWithFrequencyAndCostETL = ETLBillablesWithFrequencyAndCost.PatientReferedToAnotherFacilityETL()
    billablesWithFrequencyAndCostETL.process()
    memberFacilityUtilizationRateETL = ETLMemberFacilityUtilizationRate.MemberUtilizationRateETL()
    memberFacilityUtilizationRateETL.process()
    numberOfContractedFacilitiesETL = ETLNumberOfContractedFacilities.NumberOfContractedFacilitiesETL()
    numberOfContractedFacilitiesETL.process()
    patientReferedToAnotherFacilityETL = ETLPatientReferedToAnotherFacility.PatientReferedToAnotherFacilityETL()
    patientReferedToAnotherFacilityETL.process()
    totalHealthFacilityVisitByMemberETL = ETLTotalHealthFacilityVisitByMember.TotalHealthFacilityVisitByMembersETL()
    totalHealthFacilityVisitByMemberETL.process()
    valueAndCountOfClaimTypesETL = ETLValueAndCountOfClaimTypes.ValueAndCountOfClaimTypesETL()
    valueAndCountOfClaimTypesETL.process()
    print("Claim ETL process executed.")