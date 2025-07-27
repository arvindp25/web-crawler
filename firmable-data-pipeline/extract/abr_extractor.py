import requests
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Generator, Dict, List

ABR_SPLIT_ZIP_URL = (
    "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/"
    "resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip"
)

CACHE_DIR = Path("data/cache")
EXTRACT_DIR = CACHE_DIR / "abr_xmls"
ZIP_PATH = CACHE_DIR / "ABR_SPLIT.zip"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
EXTRACT_DIR.mkdir(parents=True, exist_ok=True)


def download_and_extract_abr_zip(url: str = ABR_SPLIT_ZIP_URL) -> List[Path]:
    """~
    Downloads and extracts ABR XML ZIP file. Skips if files already exist.

    Returns:
        List[Path]: List of extracted XML file paths.
    """
    if not ZIP_PATH.exists():
        print(f"📥 Downloading ABR ZIP from: {url}")
        response = requests.get(url)
        response.raise_for_status()
        with open(ZIP_PATH, "wb") as f:
            f.write(response.content)
    else:
        print("✅ ZIP file already downloaded.")

    xml_paths = []
    with zipfile.ZipFile(ZIP_PATH, "r") as zip_ref:
        for file in zip_ref.namelist():
            if file.endswith(".xml"):
                dest_path = EXTRACT_DIR / Path(file).name
                if not dest_path.exists():
                    print(f"📦 Extracting {file}")
                    with zip_ref.open(file) as xml_file, open(dest_path, "wb") as f:
                        f.write(xml_file.read())
                else:
                    print(f"✅ Already extracted: {file}")
                xml_paths.append(dest_path)

    return xml_paths

def parse_abr_xml(file_path: Path) -> Generator[Dict, None, None]:
    context = ET.iterparse(file_path, events=("start", "end"))
    _, root = next(context)

    for event, elem in context:
        if event == "end" and elem.tag == "ABR":
            abn = elem.findtext("ABN")
            entity_type = elem.findtext("EntityType/EntityTypeText")
            entity_status = elem.find("ABN").attrib.get("status")
            start_date = elem.find("ABN").attrib.get("ABNStatusFromDate")
            record_updated = elem.attrib.get("recordLastUpdatedDate")

            # Entity Name
            name_elem = elem.find(".//MainEntity/NonIndividualName/NonIndividualNameText")
            if name_elem is None:
                # Individual
                given_names = elem.findall(".//IndividualName/GivenName")
                family_name = elem.findtext(".//IndividualName/FamilyName")
                if given_names and family_name:
                    given = " ".join(g.text for g in given_names if g.text)
                    name = f"{given} {family_name}"
                else:
                    name = None
            else:
                name = name_elem.text

            # Address
            state = elem.findtext(".//BusinessAddress/AddressDetails/State")
            postcode = elem.findtext(".//BusinessAddress/AddressDetails/Postcode")
            address = None  # No address line in XML, can build from state + postcode optionally

            yield {
                "abn": abn,
                "entity_name": name,
                "entity_type": entity_type,
                "entity_status": entity_status,
                "start_date": start_date,
                "address": address,
                "state": state,
                "postcode": postcode,
                "record_updated": record_updated
            }

            root.clear()




def extract_abr_records(xml_paths: List[Path], max_files: int = None) -> Generator[Dict, None, None]:
    for i, xml_path in enumerate(xml_paths):
        if max_files is not None and i >= max_files:
            break
        yield from parse_abr_xml(xml_path)
