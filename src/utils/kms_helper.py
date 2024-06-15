"""
KMS Helper — field-level encryption and decryption for PCI-DSS compliance.
Encrypts PII fields (card numbers, account IDs) using AWS KMS CMK.
Pramod Vishnumolakala — github.com/pramod-vishnumolakala
"""

import base64
import logging
import boto3
from functools import lru_cache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KMS_KEY_ALIAS = "alias/fraud-detection-pipeline"
REGION        = "us-east-1"


@lru_cache(maxsize=1)
def get_kms_client():
    return boto3.client("kms", region_name=REGION)


@lru_cache(maxsize=1)
def get_key_id() -> str:
    kms = get_kms_client()
    resp = kms.describe_key(KeyId=KMS_KEY_ALIAS)
    return resp["KeyMetadata"]["KeyId"]


def encrypt_field(plaintext: str) -> str:
    """
    Encrypt a single field value using AWS KMS.
    Returns base64-encoded ciphertext safe for storage.
    """
    if not plaintext:
        return plaintext
    try:
        kms = get_kms_client()
        resp = kms.encrypt(
            KeyId=get_key_id(),
            Plaintext=plaintext.encode("utf-8"),
            EncryptionContext={"purpose": "pci-dss-field-encryption"},
        )
        return base64.b64encode(resp["CiphertextBlob"]).decode("utf-8")
    except Exception as exc:
        logger.error(f"KMS encrypt error: {exc}")
        raise


def decrypt_field(ciphertext_b64: str) -> str:
    """
    Decrypt a KMS-encrypted field value.
    Returns original plaintext string.
    """
    if not ciphertext_b64:
        return ciphertext_b64
    try:
        kms = get_kms_client()
        ciphertext = base64.b64decode(ciphertext_b64)
        resp = kms.decrypt(
            CiphertextBlob=ciphertext,
            EncryptionContext={"purpose": "pci-dss-field-encryption"},
        )
        return resp["Plaintext"].decode("utf-8")
    except Exception as exc:
        logger.error(f"KMS decrypt error: {exc}")
        raise


def mask_card_number(card_number: str) -> str:
    """Mask card number — show only last 4 digits. PCI-DSS compliant."""
    if not card_number:
        return card_number
    cleaned = card_number.replace(" ", "").replace("-", "")
    return f"****-****-****-{cleaned[-4:]}"


def mask_account_id(account_id: str) -> str:
    """Partial mask for account ID — show first 3 and last 3 chars."""
    if not account_id or len(account_id) < 6:
        return "***"
    return f"{account_id[:3]}***{account_id[-3:]}"


def encrypt_pii(value: str) -> str:
    """Alias for encrypt_field — used in producer for PII fields."""
    return encrypt_field(value)
