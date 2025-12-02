"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import zipfile
from pathlib import Path
import pandas as pd


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    
    input_dir = Path(__file__).resolve().parents[1] / "files" / "input"
    output_dir = Path(__file__).resolve().parents[1] / "files" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    dataframes = []

    for zip_entry in input_dir.iterdir():
        if zip_entry.suffixes[-2:] != [".csv", ".zip"]:
            continue
        with zipfile.ZipFile(zip_entry, "r") as archive:
            csv_name = archive.namelist()[0]
            with archive.open(csv_name) as handle:
                dataframes.append(pd.read_csv(handle))
    
    if not dataframes:
        raise FileNotFoundError("No csv.zip files were found under files/input")

    # Concatenate all dataframes
    data = pd.concat(dataframes, ignore_index=True)
    data.drop(columns=['Unnamed: 0'], inplace=True)
    
    # Create client.csv
    client = data[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
    # job: cambiar "." por "" y "-" por "_"
    client["job"] = client["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    # education: cambiar "." por "_" y "unknown" por pd.NA
    client["education"] = client["education"].str.replace(".", "_", regex=False)
    client["education"] = client["education"].replace("unknown", pd.NA)
    # credit_default: convertir "yes" a 1 y cualquier otro valor a 0
    client["credit_default"] = client["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    # mortgage: convertir "yes" a 1 y cualquier otro valor a 0
    client["mortgage"] = client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
    
    # Create campaign.csv
    campaign = data[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"]].copy()
    # previous_outcome: cambiar "success" por 1 y cualquier otro valor a 0
    campaign["previous_outcome"] = campaign["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    # campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)
    # last_contact_day: crear valor con formato "YYYY-MM-DD" combinando day y month con año 2022
    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }
    campaign["last_contact_date"] = (
        "2022-"
        + campaign["month"].str.lower().map(month_map)
        + "-"
        + campaign["day"].astype(str).str.zfill(2)
    )
    # Select final columns in correct order
    campaign = campaign[[
        "client_id",
        "number_contacts",
        "contact_duration",
        "previous_campaign_contacts",
        "previous_outcome",
        "campaign_outcome",
        "last_contact_date",
    ]]
    
    # Create economics.csv
    economics = data[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
    
    # Save to CSV files
    client.to_csv(output_dir / "client.csv", index=False)
    campaign.to_csv(output_dir / "campaign.csv", index=False)
    economics.to_csv(output_dir / "economics.csv", index=False)


if __name__ == "__main__":
    clean_campaign_data()
