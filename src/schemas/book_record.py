"""
Pydantic data contract for book registration records submitted by publishers.
Used at the Ingestion stage to validate records before they are published to Kafka.
"""
from pydantic import BaseModel, Field, field_validator
from datetime import datetime


class BookRecord(BaseModel):
    book_id: int = Field(gt=0)
    isbn: str
    title: str
    author: str
    publisher_name: str
    publisher_city: str
    category: str
    language: str
    publication_year: int
    submission_date: str

    @field_validator("isbn")
    @classmethod
    def validate_isbn(cls, value):
        if not value:
            raise ValueError("ISBN cannot be empty.")
        return value

    @field_validator("title")
    @classmethod
    def validate_title(cls, value):
        if not value.strip():
            raise ValueError("Title cannot be empty.")
        return value

    @field_validator("language")
    @classmethod
    def validate_language(cls, value):
        if value not in ["Arabic", "English"]:
            raise ValueError("Language must be Arabic or English.")
        return value

    @field_validator("publication_year")
    @classmethod
    def validate_year(cls, value):
        current_year = datetime.now().year
        if value > current_year:
            raise ValueError("Publication year cannot be in the future.")
        return value

    @field_validator("submission_date")
    @classmethod
    def validate_date(cls, value):
        datetime.strptime(value, "%Y-%m-%d")
        return value
