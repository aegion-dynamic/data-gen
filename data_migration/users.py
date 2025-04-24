from __future__ import annotations
import csv
from dataclasses import dataclass
from pathlib import Path
import boto3
from pydantic import BaseModel, Field
from typing import List, Optional

class CognitoConfig(BaseModel):
    user_pool_id: str = Field(..., description="The ID of the Cognito user pool.")
    client_id: str = Field(..., description="The client ID for the Cognito application.")
    client_secret: str = Field(..., description="The client secret for the Cognito application.")
    region: str = Field(..., description="The AWS region where the Cognito user pool is located.")
    identity_pool_id: str = Field(..., description="The ID of the Cognito identity pool.")


@dataclass
class UserModel:
    email: str
    first_name: str
    last_name: str
    username: Optional[str] = None
    phone_number: Optional[str] = None
    external_id: Optional[str] = None

    @staticmethod
    def from_csv(file_path: Path) -> List[UserModel]:
        users = []
        # Read the CSV file and create a list of UserModel instances
        with file_path.open("r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                user = UserModel(
                    username=row["username"],
                    first_name=row["first_name"],
                    last_name=row["last_name"],
                    email=row["email"],
                    phone_number=row.get("phone_number"),
                )
                users.append(user)

        return users


def create_cognito_users(users: List[UserModel], cognito_config: CognitoConfig):
    client = boto3.client("cognito-idp", region_name=cognito_config.region)

    for user in users:
        # Check if the user exists
        try:
            response = client.admin_get_user(
                UserPoolId=cognito_config.user_pool_id,
                email=user.email,
            )
            print(f"User {user.username} already exists with ID: {response['User']['Attributes']}")
            user.external_id = next(
                (attr["Value"] for attr in response["User"]["Attributes"] if attr["Name"] == "sub"),
                None,
            )
        except client.exceptions.UserNotFoundException:
            # User does not exist, create a new user
            pass
        except Exception as e:
            print(f"Error checking user {user.username}: {e}")
            continue
        # Create the user if it does not exist
        try:
            response = client.admin_create_user(
                UserPoolId=cognito_config.user_pool_id,
                Username=user.username,
                UserAttributes=[
                    {"Name": "email", "Value": user.email},
                    {"Name": "phone_number", "Value": user.phone_number or ""},
                    {"Name": "given_name", "Value": user.first_name},
                    {"Name": "family_name", "Value": user.last_name},
                ],
                MessageAction="SUPPRESS",
            )
            # Extract the Cognito-generated ID (sub) and attach it to the user's external_id
            user.external_id = next(
                (attr["Value"] for attr in response["User"]["Attributes"] if attr["Name"] == "sub"),
                None,
            )
            print(f"User {user.username} created successfully with ID: {user.external_id}")
        except Exception as e:
            print(f"Error creating user {user.username}: {e}")


def update_cognito_users(users: List[UserModel], file_path: Path):
    # Write the updated user data back to the CSV file
    with file_path.open("w", newline="") as csvfile:
        fieldnames = ["username", "first_name", "last_name", "email", "phone_number", "external_id"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for user in users:
            writer.writerow({
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "email": user.email,
                "phone_number": user.phone_number or "",
                "external_id": user.external_id or "",
            })