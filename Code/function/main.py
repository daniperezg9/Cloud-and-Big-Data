import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import storage
import io

def generate_diagram(df, x_column, y_column):
    num_elements = len(df[x_column])
    #Convertimos los valores a string para que no haga interprete los datos del eje x como enteros en caso de que lo sean
    #porque si no se inventa valores en el rango del eje x
    df[x_column] = df[x_column].astype(str)
    plt.figure(figsize = (max(num_elements * 0.25, 10), max(num_elements * 0.15, 10)))
    plt.tight_layout()
    plt.xticks(rotation=45, ha='right')
    plt.xlabel(x_column)
    plt.ylabel(y_column)
    plt.title(f"{x_column} - {y_column} diagram")
    plt.bar(df[x_column], df[y_column])
    
    img_buffer = io.BytesIO()
    plt.savefig(f"{x_column}_{y_column}_diagram.png", format='png')
    plt.savefig(img_buffer, format='png')
    img_buffer.seek(0)
    plt.close() 
    
    return img_buffer

def upload_to_bucket(blob_name, bucket_name, file_data):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(file_data, content_type='image/png')
    file_data.close()

def gcs(event, context):
    file_data = event
    bucket_name = file_data['bucket']
    file_name = file_data['name']

    storage_client = storage.Client()
    
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_bytes()
    data = io.BytesIO(data)

    df = pd.read_csv(data)
    columns = df.columns
    
    for i in range(1, (len(columns))):
        x_column = columns[0]
        y_column = columns[i]
        img_buffer = generate_diagram(df, x_column, y_column)
        if img_buffer:
            output_blob_name = f"diagrams/{x_column}-{y_column}-diagram.png"
            upload_to_bucket(output_blob_name, bucket_name, img_buffer)
   