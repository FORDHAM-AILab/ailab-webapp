from __future__ import print_function
import logging
import os.path
from typing import List
import io
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from fermi_backend.webapp.utils.data.data_utils import  decode_bytes_obj

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly', 'https://www.googleapis.com/auth/drive']

import logging
logger = logging.getLogger(__name__)

class GoogleDriveAPI:
    def __init__(self, cred_dir_path:str):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        token_path, cred_path = os.path.join(cred_dir_path, 'token.json'), os.path.join(cred_dir_path,
                                                                                        'credentials.json')
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    cred_path, SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        self.service = build('drive', 'v3', credentials=creds)


    def get_obj(self, is_dir: bool = None, names:List[str]=None, mode='contains', conjunction=True, parent_id: str=None):
        """
            Args:
                is_dir: True for dir, False for not dir, None for both
                names: A list of strings for names
                mode: one of contains, not, and =. 'contains' for contains, 'not' for not contains, '=' for exact match
                conjunction: for the list of names, True for and, False for or
                parent_id: if specified, will return objects under the folder specified by this id
        """

        try:
            if is_dir is None:
                dir_arg = ""
            elif is_dir:
                dir_arg = "mimeType = 'application/vnd.google-apps.folder'"
            else:
                dir_arg = "mimeType != 'application/vnd.google-apps.folder'"

            if names:
                q_name = ""
                for idx,name in enumerate(names):
                    if mode=='not':
                        q_name += f"not name contains '{name}'"
                    else:
                        q_name += f"name {mode} '{name}'"
                    if 0 < idx < len(names) - 1:
                        q_name += 'and' if conjunction else 'or'
                if is_dir is not None:
                    q = dir_arg + f" and ({q_name})"
                else:
                    q = q_name
            else:
                q = dir_arg

            if parent_id:
                q += f" and ({parent_id} in parents)" if q else f"'{parent_id}' in parents"
            results = self.service.files().list(
                q=q, fields="nextPageToken, files(id, name)").execute()
            items = results.get('files', [])

            if not items:
                print('No files found.')
                return
            list_res = []
            for item in items:
                list_res.append((item['name'], item['id']))
            return list_res
        except HttpError as error:
            # TODO(developer) - Handle errors from drive API.
            print(f'An error occurred: {error}')


    def get_files_under_parent_name(self, parent_name: str, mode='contains'):
        parent_name, parent_id = self.get_obj(is_dir=True, names=[parent_name], mode=mode)[0]
        logger.info(f'get parent: {parent_name}, {parent_id}')
        files = self.get_obj(is_dir=None, names=None, parent_id=parent_id)
        return files


    def download_file(self, real_file_id):
        """Downloads a file
        Args:
            service
            real_file_id: ID of the file to download
        Returns : IO object with location.
        # TODO: For large file, see https://stackoverflow.com/questions/27617258/memoryerror-how-to-download-large-file-via-google-drive-sdk-using-python
        """

        try:
            # create drive api client

            file_id = real_file_id

            # pylint: disable=maybe-no-member
            request = self.service.files().get_media(fileId=file_id)
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(F'Download {int(status.progress() * 100)}.')

        except HttpError as error:
            print(F'An error occurred: {error}')
            file = None

        return file.getvalue()


    def get_csv_file(self, fname:str, path_loc:str = None):
        assert fname.endswith('.csv'), "Unsupported csv extension"
        _, f_id = self.get_obj(is_dir=False, names=[fname], mode="=")[0]
        file = self.download_file(f_id)
        return decode_bytes_obj(file, 'csv', os.path.join(path_loc, fname) if path_loc else None)


if __name__ == '__main__':
    driver = GoogleDriveAPI('/Users/xuanmingcui/Documents/projects/ailab-webapp/fermi_backend/webapp/utils/data')
    driver.get_csv_file('DJ30-5yr-stocks.csv', "./")
    # download_file(service, '')
