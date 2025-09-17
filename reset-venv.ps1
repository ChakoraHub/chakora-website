# Kill any running Python processes
Get-Process python -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.Id -Force }

# Deactivate venv if active
deactivate 2>$null

# Force delete old venv
cmd /c "rmdir /s /q .venv"

# Create new venv with Python 3.12
py -3.12 -m venv .venv

# Activate new venv
. .\.venv\Scripts\Activate.ps1

# Upgrade pip
python -m pip install --upgrade pip

# Install kivy
python -m pip install kivy

# Show confirmation
python --version
python -m pip show kivy
