from database.crud import *
from fastapi import FastAPI, HTTPException

app=FastAPI()

@app.get("/importanza/{anno_da}-{anno_a}")
async def get_importanza_by_anno(anno_da:int, anno_a:int):
    if(anno_a < anno_da):
        raise HTTPException(status_code=404, detail="A ANNO deve essere maggiore di DA ANNO")
    return read_importanza_by_anno(anno_da, anno_a)

@app.get("/andamento/{anno_da}-{anno_a}")
async def get_andamento_by_anno(anno_da:int, anno_a:int):
    if(anno_a < anno_da):
        raise HTTPException(status_code=404, detail="A ANNO deve essere maggiore di DA ANNO")
    return read_andamento_by_anno(anno_da, anno_a)

@app.get("/produttivita/{anno_da}-{anno_a}")
async def get_produttivita_by_anno(anno_da:int, anno_a:int):
    if(anno_a < anno_da):
        raise HTTPException(status_code=404, detail="A ANNO deve essere maggiore di DA ANNO")
    return read_produttività_by_anno(anno_da, anno_a)

@app.get("/serie-calcolate/{anno_da}-{anno_a}")
async def get_serie_by_anno(anno_da:int, anno_a:int):
    if(anno_a < anno_da):
        raise HTTPException(status_code=404, detail="A ANNO deve essere maggiore di DA ANNO")
    return read_serie_calcolate_by_anno(anno_da, anno_a)