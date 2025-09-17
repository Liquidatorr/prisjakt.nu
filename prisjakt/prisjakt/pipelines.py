import os
import tempfile
import pandas as pd
from datetime import datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import googleapiclient.discovery
import googleapiclient.errors
import googleapiclient.http
from scrapy.exceptions import DropItem


class PrisjaktExportPipeline:
    def open_spider(self, spider):
        spider.logger.info("Prisjakt Export Pipeline is starting...")
        self.items = []
        self.session_folder = tempfile.mkdtemp(prefix=f"{spider.name}_")
        spider.logger.info(f"Temporary export folder: {self.session_folder}")

    def process_item(self, item, spider):
        if not item.get("product_id") or not item.get("product_title") or not item.get("product_url"):
            spider.logger.error(f"❌ Missing required fields for item: {item}")
            raise DropItem(f"Missing fields for item: {item}")
        self.items.append(item)
        return item

    def close_spider(self, spider):
        if not self.items:
            spider.logger.warning("⚠️ Geen items om te exporteren.")
            return

        df = pd.DataFrame(self.items)
        today = datetime.now().strftime("%Y-%m-%d")
        excel_file = os.path.join(self.session_folder, f"{spider.name}_products_{today}.xlsx")

        # Excel opslaan
        spider.logger.info(f"[DEBUG] Writing Excel: {excel_file}")
        df.to_excel(excel_file, index=False)
        spider.logger.info(f"✅ Data exported to {excel_file}")

        # Upload naar Google Drive
        self.upload_to_google_drive(spider, excel_file)

    def upload_to_google_drive(self, spider, file_path):
        credentials_path = "/home/louverius/projects/tokens/credentials.json"
        token_path = f"/home/louverius/projects/tokens/{spider.name}_token.json"
        creds = None

        spider.logger.info(f"[DEBUG] credentials.json path: {credentials_path}")
        spider.logger.info(f"[DEBUG] token.json path: {token_path}")

        if not os.path.exists(credentials_path):
            spider.logger.error(f"❌ credentials.json not found at {credentials_path}")
            return

        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(
                token_path, ["https://www.googleapis.com/auth/drive.file"]
            )
        else:
            spider.logger.error(f"❌ token.json not found at {token_path}")
            return

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                spider.logger.info("🔄 Token refreshed.")
            else:
                spider.logger.error("❌ Missing or invalid credentials and cannot refresh token.")
                return

        try:
            service = googleapiclient.discovery.build("drive", "v3", credentials=creds)
            folder_id = self.get_or_create_folder(service, "Scraping")
            subfolder_id = self.get_or_create_folder(service, spider.name, parent_id=folder_id)

            spider.logger.info(f"[DEBUG] Upload target folder: Scraping/{spider.name}")
            spider.logger.info(f"[DEBUG] Folder ID: {folder_id}, Subfolder ID: {subfolder_id}")

            file_metadata = {"name": os.path.basename(file_path), "parents": [subfolder_id]}
            media = googleapiclient.http.MediaFileUpload(
                file_path,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            uploaded_file = service.files().create(
                body=file_metadata, media_body=media, fields="id"
            ).execute()
            spider.logger.info(f"✅ File uploaded to Google Drive with ID: {uploaded_file.get('id')}")
            spider.logger.info(f"📂 View file: https://drive.google.com/file/d/{uploaded_file.get('id')}/view")
        except googleapiclient.errors.HttpError as error:
            spider.logger.error(f"❌ Google Drive upload failed: {error}")

    def get_or_create_folder(self, service, name, parent_id=None):
        query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"

        results = service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
        folders = results.get("files", [])
        if folders:
            return folders[0]["id"]

        metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
        if parent_id:
            metadata["parents"] = [parent_id]
        folder = service.files().create(body=metadata, fields="id").execute()
        return folder["id"]