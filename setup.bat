@echo on

:: Create virtual environment
python -m venv venv

:: Activate virtual environment
call venv\Scripts\activate

:: Install dependencies
pip install --upgrade pip
pip install -r requeriments.txt

echo Setup complete. Activate the virtual environment with 'venv\Scripts\activate'.