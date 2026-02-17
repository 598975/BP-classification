"""
Sync SQLite database data to Algolia for fuzzy search capabilities.
Uses the blueprints_categorized table for categorized and clustered blueprints.
"""

import os
import re
import sys
from pathlib import Path
from dotenv import load_dotenv
from algoliasearch.search.client import SearchClientSync
from tqdm import tqdm

# Add the parent directory to the path to import modules
sys.path.append(str(Path(__file__).parent.parent))
from db.database import Database
from db.models import BlueprintCategorized

load_dotenv()


class AlgoliaSync:
    def __init__(self, index_name="test_MSc"):
        """Initialize Algolia client and database connection."""
        algolia_app_id = os.getenv("ALGOLIA_APP_ID")
        algolia_admin_key = os.getenv("ALGOLIA_ADMIN_KEY")
        
        if not algolia_app_id or not algolia_admin_key:
            raise ValueError(
                "Please set ALGOLIA_APP_ID and ALGOLIA_ADMIN_KEY in your .env file"
            )
        
        self.client = SearchClientSync(algolia_app_id, algolia_admin_key)
        self.index_name = index_name
        self.db = Database()
    
    def configure_index_settings(self):
        """Configure Algolia index settings for optimal search."""
        self.client.set_settings(
            index_name=self.index_name,
            index_settings={
                "searchableAttributes": [
                    "name,description",
                    "blueprint_code_snippet",
                    "features"
                ],
                "attributesForFaceting": [
                    "topic_id",
                    "fine_cluster",
                    "top_cluster",
                    "inputs",
                    "outputs",
                    "searchable(features)"
                ],
                "advancedSyntax": True,
                "typoTolerance": True,
                "minWordSizefor1Typo": 4,
                "minWordSizefor2Typos": 8,
            }
        )
        print("✓ Index settings configured")
    
    def prepare_blueprint_record(self, blueprint):
        """Transform database models into Algolia record."""
        # Truncate large fields to stay under Algolia"s 10KB limit
        # Leave room for other fields (~2KB), so limit text fields to ~8KB total
        
        def truncate_text(text, max_bytes=3000):
            """Truncate text to max bytes while preserving UTF-8 encoding."""
            if not text:
                return ""
            if len(text.encode("utf-8")) <= max_bytes:
                return text
            # Truncate and add ellipsis
            truncated = text.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore")
            return truncated + "..."
        
        # Truncate blueprint_code significantly - keep only a snippet for search
        blueprint_code = blueprint.blueprint_code or ""
        blueprint_code_snippet = truncate_text(blueprint_code, max_bytes=1500)
        
        # Truncate description
        description = blueprint.description or ""
        description_truncated = truncate_text(description, max_bytes=1000)
        
        features = blueprint.features.split(" ") if blueprint.features else []
        features = [item for item in features if item != "<PAD>"]
        feature_list = []
        inputs = []
        outputs = []
        
        for kwd in features:
            in_out = re.search(r"(input__|output__)(input_|output_)?", kwd)
            kwd = kwd.removeprefix(in_out.group()) if in_out else kwd
            kwd = kwd.replace("_", " ").strip()
            in_out = in_out.group(1) if in_out else None
            feature_list.append(kwd)
            if in_out == "input__":
                inputs.append(kwd)
            elif in_out == "output__":
                outputs.append(kwd)               

        
        record = {
            "objectID": str(blueprint.id),
            "blueprint_id": blueprint.id,
            "blueprint_hash": blueprint.blueprint_hash,
            "name": blueprint.name or "",
            "description": description_truncated,
            "blueprint_code_snippet": blueprint_code_snippet,
            "blueprint_url": blueprint.blueprint_url or "",
            "inputs": inputs,
            "outputs": outputs,
            
            # Clustering information
            "fine_cluster": blueprint.fine_cluster,
            "top_cluster": blueprint.top_cluster,
            "features": feature_list,
            
            # Basic metadata from blueprints_categorized
            "post_id": blueprint.post_id,
            "topic_id": blueprint.topic_id,
            "created_at": blueprint.created_at.isoformat() if blueprint.created_at else None,
        }
        
        return record
    
    def sync_blueprints(self, batch_size=1000):
        """Sync all blueprints from SQLite to Algolia using blueprints_categorized table."""
        session = self.db.open_session()
        
        try:
            # Query all blueprints_categorized
            query = session.query(BlueprintCategorized)
            
            total_count = query.count()
            print(f"Syncing {total_count} blueprints from blueprints_categorized to Algolia...")
            
            records = []
            skipped = 0
            synced = 0
            
            with tqdm(total=total_count) as pbar:
                for blueprint in query:
                    try:
                        record = self.prepare_blueprint_record(blueprint)
                        
                        # Check estimated record size (rough estimate)
                        record_size = len(str(record).encode("utf-8"))
                        if record_size > 9500:  # Leave some buffer under 10KB
                            skipped += 1
                            pbar.update(1)
                            continue
                        
                        records.append(record)
                        
                        # Send batch when reaching batch_size
                        if len(records) >= batch_size:
                            try:
                                self.client.save_objects(index_name=self.index_name, objects=records)
                                synced += len(records)
                                pbar.update(len(records))
                                records = []
                            except Exception as batch_error:
                                print(f"\n⚠ Error with batch: {batch_error}")
                                # Try individual records to find the problematic one
                                for record in records:
                                    try:
                                        self.client.save_objects(index_name=self.index_name, objects=[record])
                                        synced += 1
                                        pbar.update(1)
                                    except Exception:
                                        skipped += 1
                                        pbar.update(1)
                                records = []
                    except Exception as e:
                        print(f"\n⚠ Error processing blueprint {blueprint.id}: {e}")
                        skipped += 1
                        pbar.update(1)
                        continue
                
                # Send remaining records
                if records:
                    try:
                        self.client.save_objects(index_name=self.index_name, objects=records)
                        synced += len(records)
                        pbar.update(len(records))
                    except Exception as batch_error:
                        print(f"\n⚠ Error with final batch: {batch_error}")
                        for record in records:
                            try:
                                self.client.save_objects(index_name=self.index_name, objects=[record])
                                synced += 1
                                pbar.update(1)
                            except Exception:
                                skipped += 1
                                pbar.update(1)
            
            print(f"✓ Successfully synced {synced} blueprints to Algolia")
            if skipped > 0:
                print(f"⚠ Skipped {skipped} blueprints (too large or errors)")
            
        except Exception as e:
            print(f"Error syncing to Algolia: {e}")
            raise
        finally:
            session.close()
    
    def clear_index(self):
        """Clear all records from the Algolia index."""
        self.client.clear_objects(index_name=self.index_name)
        print("✓ Index cleared")


def main():
    """Main function to run the sync."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Sync SQLite data to Algolia from blueprints_categorized table")
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear the index before syncing"
    )
    parser.add_argument(
        "--index",
        type=str,
        default="test_MSc",
        help="Algolia index name (default: test_MSc)"
    )
    
    args = parser.parse_args()
    
    syncer = AlgoliaSync(index_name=args.index)
    
    if args.clear:
        syncer.clear_index()
    
    syncer.configure_index_settings()
    syncer.sync_blueprints()


if __name__ == "__main__":
    main()
