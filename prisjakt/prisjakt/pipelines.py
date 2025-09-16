class PrisjaktExportPipeline:
    def open_spider(self, spider):
        self.items = []

    def process_item(self, item, spider):
        self.items.append(item)
        return item

    def close_spider(self, spider):
        import os
        import pandas as pd
        from datetime import datetime

        # Bestand maken
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"{spider.name}_products_{today}.xlsx"
        df = pd.DataFrame(self.items)
        df.to_excel(filename, index=False)

        spider.logger.info(f"✅ Data exported to {filename}")

        # Upload naar Google Drive (zelfde flow als je Tweakers pipeline)
        from prisjakt.upload_google import upload_file
        upload_file(filename, spider)
