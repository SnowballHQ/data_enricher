"""
Google Authentication Manager - Persistent OAuth credentials
"""

import os
import json
import pickle
from typing import Optional
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
import streamlit as st

class GoogleAuthManager:
    """Manages persistent Google OAuth authentication"""
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    CREDENTIALS_FILE = '.google_credentials.json'
    TOKEN_FILE = '.google_token.pickle'
    
    def __init__(self):
        self.credentials = None
        self.credentials_path = os.path.join(os.getcwd(), self.CREDENTIALS_FILE)
        self.token_path = os.path.join(os.getcwd(), self.TOKEN_FILE)
    
    def save_client_credentials(self, credentials_json: dict) -> bool:
        """Save OAuth client credentials to disk"""
        try:
            with open(self.credentials_path, 'w') as f:
                json.dump(credentials_json, f)
            return True
        except Exception as e:
            st.error(f"Failed to save credentials: {e}")
            return False
    
    def load_client_credentials(self) -> Optional[dict]:
        """Load OAuth client credentials from disk"""
        try:
            if os.path.exists(self.credentials_path):
                with open(self.credentials_path, 'r') as f:
                    return json.load(f)
            return None
        except Exception as e:
            print(f"Failed to load client credentials: {e}")
            return None
    
    def save_token(self, credentials: Credentials) -> bool:
        """Save OAuth token to disk"""
        try:
            with open(self.token_path, 'wb') as f:
                pickle.dump(credentials, f)
            return True
        except Exception as e:
            print(f"Failed to save token: {e}")
            return False
    
    def load_token(self) -> Optional[Credentials]:
        """Load OAuth token from disk"""
        try:
            if os.path.exists(self.token_path):
                with open(self.token_path, 'rb') as f:
                    credentials = pickle.load(f)
                    return credentials
            return None
        except Exception as e:
            print(f"Failed to load token: {e}")
            return None
    
    def is_authenticated(self) -> bool:
        """Check if we have valid authentication"""
        if not self.credentials:
            self.credentials = self.load_token()
        
        if not self.credentials:
            return False
        
        # Check if credentials are valid
        if not self.credentials.valid:
            if self.credentials.expired and self.credentials.refresh_token:
                try:
                    self.credentials.refresh(Request())
                    self.save_token(self.credentials)
                    return True
                except Exception as e:
                    print(f"Failed to refresh token: {e}")
                    return False
            return False
        
        return True
    
    def get_credentials(self) -> Optional[Credentials]:
        """Get valid credentials"""
        if self.is_authenticated():
            return self.credentials
        return None
    
    def authenticate_new(self, credentials_json: dict) -> bool:
        """Perform new OAuth authentication"""
        try:
            # Save client credentials for future use
            self.save_client_credentials(credentials_json)
            
            # Create OAuth flow
            flow = Flow.from_client_config(
                credentials_json,
                scopes=self.SCOPES,
                redirect_uri='urn:ietf:wg:oauth:2.0:oob'
            )
            
            # Get authorization URL
            auth_url, _ = flow.authorization_url(prompt='consent')
            
            # Store flow in session for the callback
            st.session_state.oauth_flow = flow
            st.session_state.auth_url = auth_url
            
            return True
            
        except Exception as e:
            st.error(f"Authentication setup failed: {e}")
            return False
    
    def complete_authentication(self, auth_code: str) -> bool:
        """Complete OAuth authentication with authorization code"""
        try:
            if 'oauth_flow' not in st.session_state:
                st.error("No authentication flow found. Please restart authentication.")
                return False
            
            flow = st.session_state.oauth_flow
            
            # Exchange code for credentials
            flow.fetch_token(code=auth_code)
            self.credentials = flow.credentials
            
            # Save token for future use
            if self.save_token(self.credentials):
                st.success("✅ Authentication completed and saved!")
                
                # Clean up session
                if 'oauth_flow' in st.session_state:
                    del st.session_state.oauth_flow
                if 'auth_url' in st.session_state:
                    del st.session_state.auth_url
                
                return True
            else:
                st.error("Failed to save authentication token")
                return False
                
        except Exception as e:
            st.error(f"Authentication failed: {e}")
            return False
    
    def revoke_authentication(self) -> bool:
        """Revoke and clear stored authentication"""
        try:
            # Remove stored files
            if os.path.exists(self.token_path):
                os.remove(self.token_path)
            
            if os.path.exists(self.credentials_path):
                os.remove(self.credentials_path)
            
            # Clear in-memory credentials
            self.credentials = None
            
            # Clear session state
            if 'oauth_flow' in st.session_state:
                del st.session_state.oauth_flow
            if 'auth_url' in st.session_state:
                del st.session_state.auth_url
            
            return True
            
        except Exception as e:
            st.error(f"Failed to revoke authentication: {e}")
            return False
    
    def get_auth_status(self) -> dict:
        """Get detailed authentication status"""
        status = {
            'authenticated': False,
            'has_client_credentials': False,
            'has_token': False,
            'token_valid': False,
            'token_expired': False,
            'needs_refresh': False
        }
        
        # Check client credentials
        status['has_client_credentials'] = os.path.exists(self.credentials_path)
        
        # Check token
        status['has_token'] = os.path.exists(self.token_path)
        
        if status['has_token']:
            credentials = self.load_token()
            if credentials:
                status['token_valid'] = credentials.valid
                status['token_expired'] = credentials.expired if hasattr(credentials, 'expired') else False
                status['needs_refresh'] = status['token_expired'] and bool(credentials.refresh_token)
        
        status['authenticated'] = self.is_authenticated()
        
        return status
    
    def setup_gitignore(self):
        """Add credential files to .gitignore"""
        gitignore_path = '.gitignore'
        credentials_entries = [
            '.google_credentials.json',
            '.google_token.pickle'
        ]
        
        try:
            # Read existing .gitignore
            existing_lines = []
            if os.path.exists(gitignore_path):
                with open(gitignore_path, 'r') as f:
                    existing_lines = f.read().splitlines()
            
            # Add missing entries
            lines_to_add = []
            for entry in credentials_entries:
                if entry not in existing_lines:
                    lines_to_add.append(entry)
            
            if lines_to_add:
                with open(gitignore_path, 'a') as f:
                    if existing_lines and not existing_lines[-1].strip():
                        pass  # Already has empty line
                    else:
                        f.write('\n')
                    
                    f.write('# Google Sheets Authentication\n')
                    for line in lines_to_add:
                        f.write(f'{line}\n')
                
                return True
            
        except Exception as e:
            print(f"Failed to update .gitignore: {e}")
        
        return False
