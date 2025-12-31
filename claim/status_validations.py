import datetime
import logging

from django.utils.translation import gettext as _

from claim.models import Claim
from .apps import ClaimConfig
from claim.utils import check_initial_status_permission, validate_status_transition

logger = logging.getLogger(__name__)

class ClaimStatusValidationRegistry:
    _validators = {}

    @classmethod
    def register(cls, from_statuses, to_status):
        def decorator(func):
            if not isinstance(from_statuses, list):
                statuses = [from_statuses]
            else:
                statuses = from_statuses
            
            for from_status in statuses:
                key = (from_status, to_status)
                if key not in cls._validators:
                    cls._validators[key] = []
                cls._validators[key].append(func)
            return func
        return decorator

    @classmethod
    def validate(cls, claim, new_status, user):
        # Default first validations
        check_initial_status_permission(claim.status, user)
        validate_status_transition(claim.status, new_status, user)

        # Registered validations
        key = (claim.status, new_status)
        if key in cls._validators:
            for validator in cls._validators[key]:
                result = validator(claim, user)
                if result is not None:
                    return result
        return None

# Specific Validators

def validate_insuree_check_in_limit(claim, user):
    if not claim.insuree or not claim.date_from:
        return None

    try:
        from insuree.models import InsureeCheckIn
        claim_date = datetime.datetime.strptime(
            str(claim.date_from), "%Y-%m-%d"
        ).date()
        thirty_days_ago = claim_date - datetime.timedelta(days=30)
        start_dt = datetime.datetime.combine(
            thirty_days_ago,
            datetime.time.min
        )
        end_dt = datetime.datetime.combine(
            claim_date,
            datetime.time.max
        )
        checkin_count = InsureeCheckIn.objects.filter(
            insuree=claim.insuree,
            check_in_date__gte=start_dt,
            check_in_date__lte=end_dt
        ).count()
        if checkin_count >= 3:
            return Claim.STATUS_FLAGGED

    except Exception:
        logger.exception(
            "validate_insuree_check_in_limit failed for claim %s",
            claim.uuid
        )
        raise



def validate_claimed_amount_limit(claim, user):
    """
    Validate if the claimed amount is more than 10,000 birr.
    """
    if claim.claimed is not None and claim.claimed > ClaimConfig.max_claimed_amount_to_be_flagged:
        return Claim.STATUS_FLAGGED
    return None


# Register Validators

@ClaimStatusValidationRegistry.register(
    [Claim.STATUS_SUBMITTED_TO_HEAD, Claim.STATUS_RESUBMITTED_TO_HEAD],
    Claim.STATUS_CHECKED
)
def validate_submission_to_checked(claim, user):
    for validator in (
        validate_insuree_check_in_limit,
        validate_claimed_amount_limit,
    ):
        result = validator(claim, user)
        if result is not None:
            return result
    return None
