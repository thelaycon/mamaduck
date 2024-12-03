from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/create-database", response_class=HTMLResponse)
async def create_database(request: Request):
    return templates.TemplateResponse("create_database.html", {"request": request})


@app.post("/create-database")
async def handle_create_database(db_name: str = Form(...)):
    # Placeholder for actual database creation logic
    return {"message": f"Database '{db_name}' created successfully!"}


@app.get("/load-data", response_class=HTMLResponse)
async def load_data(request: Request):
    return templates.TemplateResponse("load_data.html", {"request": request})


@app.post("/load-data")
async def handle_load_data(
    data_type: str = Form(...),
    file_path: str = Form(...),
):
    # Placeholder for actual data loading logic
    return {"message": f"Data from '{file_path}' loaded as {data_type} successfully!"}


@app.get("/export-database", response_class=HTMLResponse)
async def export_database(request: Request):
    return templates.TemplateResponse("export_database.html", {"request": request})


@app.post("/export-database")
async def handle_export_database(export_format: str = Form(...)):
    # Placeholder for actual export logic
    return {"message": f"Database exported as {export_format} successfully!"}
