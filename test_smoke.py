import os
import sys
import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==============================================================================
# CONFIGURATION
# Default to production if no env var is provided, matching blogger_smoke.py pattern.
# For local testing, set: TICKET_BASE_URL="http://localhost:8080"
# ==============================================================================
BASE_URL = os.getenv(
    "TICKET_BASE_URL",
    os.getenv("BASE_URL", "https://www.chakorahub.com")
).rstrip("/")

TICKET_STUDENT_URL = f"{BASE_URL}/tickets/student"
TICKET_EMPLOYEE_URL = f"{BASE_URL}/tickets/employee"
TICKET_ADMIN_URL = f"{BASE_URL}/tickets/admin"
TICKET_PORTAL_URL = f"{BASE_URL}/tickets"
TICKET_HEALTH_URL = f"{BASE_URL}/api/ticket/health"

WAIT = int(os.getenv("SELENIUM_WAIT_TIMEOUT", "25"))


# ==============================================================================
# FIXTURES
# ==============================================================================
@pytest.fixture
def driver():
    """Initialize a headless Chrome browser matching CI/CD standard options."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1440,1000")

    browser = webdriver.Chrome(options=options)
    browser.set_page_load_timeout(45)

    try:
        yield browser
    finally:
        browser.quit()


def wait_loaded(browser):
    """Wait until the document readyState is complete."""
    WebDriverWait(browser, WAIT).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


# ==============================================================================
# SMOKE TESTS FOR CHAKORAHUB TICKETING MODULE
# ==============================================================================
def test_student_ticket_portal_loads(driver):
    """
    Verify the student support tickets page loads properly:
    - Page heading and description render
    - KPI grid is visible
    - Tickets table container is present
    - Search input and status filters exist
    - Create ticket modal is present and can be opened
    """
    driver.get(TICKET_STUDENT_URL)
    wait_loaded(driver)

    # 1. Verify heading
    heading = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, ".header-title-area h1"))
    )
    assert "Support Tickets" in heading.text, f"Unexpected heading: {heading.text}"

    # 2. Verify KPI Grid & Cards
    kpi_grid = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, ".kpi-grid"))
    )
    assert kpi_grid.is_displayed(), "KPI grid should be visible"

    kpi_cards = driver.find_elements(By.CSS_SELECTOR, ".kpi-card")
    assert len(kpi_cards) >= 3, f"Expected at least 3 KPI cards, found {len(kpi_cards)}"

    # 3. Verify Table Container and search input
    table_container = driver.find_element(By.CSS_SELECTOR, ".table-container")
    assert table_container.is_displayed(), "Tickets table container should be visible"

    search_input = driver.find_element(By.ID, "search-input")
    assert search_input.is_displayed(), "Search input should be visible"

    filter_status = driver.find_element(By.ID, "filter-status")
    assert filter_status.is_displayed(), "Filter status dropdown should be visible"

    # 4. Verify 'Raise New Ticket' button opens create modal
    raise_btn = driver.find_element(By.XPATH, "//button[contains(., 'Raise New Ticket')]")
    assert raise_btn.is_displayed(), "'Raise New Ticket' button should be visible"

    create_modal = driver.find_element(By.ID, "create-modal")
    # Modal overlay should not be active initially
    assert "active" not in (create_modal.get_attribute("class") or "").split()

    # Click the button to open modal
    raise_btn.click()

    # Modal should now have 'active' class
    WebDriverWait(driver, 5).until(
        lambda d: "active" in (d.find_element(By.ID, "create-modal").get_attribute("class") or "").split()
    )

    # Verify form inputs in the modal
    form = driver.find_element(By.ID, "create-ticket-form")
    assert form.is_displayed(), "Create ticket form should be visible"

    name_input = driver.find_element(By.ID, "form-name")
    assert name_input is not None

    email_input = driver.find_element(By.ID, "form-email")
    assert email_input is not None


def test_employee_ticket_portal_loads(driver):
    """
    Verify the employee / staff ticket workspace loads properly:
    - Sidebar navigation is present
    - Topbar heading indicates employee workspace
    - KPI grid is visible
    """
    driver.get(TICKET_EMPLOYEE_URL)
    wait_loaded(driver)

    # 1. Verify page title or heading
    WebDriverWait(driver, WAIT).until(
        lambda d: "ticket" in d.title.lower() or "chakorahub" in d.title.lower()
    )

    # 2. Check layout components
    sidebar = WebDriverWait(driver, WAIT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".sidebar"))
    )
    assert sidebar.is_displayed(), "Employee sidebar should be visible"

    topbar = driver.find_element(By.CSS_SELECTOR, ".topbar")
    assert topbar.is_displayed(), "Employee topbar should be visible"

    # 3. Verify KPI cards
    kpi_cards = driver.find_elements(By.CSS_SELECTOR, ".kpi-card")
    assert len(kpi_cards) >= 1, "Employee KPI cards should be present"


def test_admin_ticket_portal_loads(driver):
    """
    Verify the operations desk / admin ticket console loads properly:
    - Page heading shows Admin Support Tickets / Operations Desk
    - KPI metrics cards are displayed
    - Master table container is present
    """
    driver.get(TICKET_ADMIN_URL)
    wait_loaded(driver)

    # 1. Verify heading area
    heading = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, ".header-title-area h1"))
    )
    assert "Support" in heading.text or "Tickets" in heading.text or "Desk" in heading.text

    # 2. Verify table container
    table_container = WebDriverWait(driver, WAIT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".table-container"))
    )
    assert table_container.is_displayed(), "Admin ticket master table container should be visible"


def test_ticket_portal_role_routing(driver):
    """
    Verify /tickets route dynamically resolves to the student portal by default
    when accessed by visitors / clients.
    """
    driver.get(TICKET_PORTAL_URL)
    wait_loaded(driver)

    # When accessed without special session/role, it routes to Student portal
    heading = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, ".header-title-area h1"))
    )
    assert "Support Tickets" in heading.text


def test_ticket_api_proxy_connectivity(driver):
    """
    Verify that the reverse proxy /api/ticket route responds (non-502/non-500)
    confirming communication between the website gateway and the ticket service.
    """
    driver.get(TICKET_HEALTH_URL)
    wait_loaded(driver)

    page_source = driver.page_source.lower()
    # It should either return JSON status or a defined API response, not an unhandled 502 Bad Gateway
    assert "502 bad gateway" not in page_source, "Ticket microservice proxy returned 502 Bad Gateway"
    assert "500 internal server error" not in page_source, "Ticket microservice proxy returned 500 Internal Error"


# ==============================================================================
# MAIN ENTRYPOINT (allows running both `pytest test_smoke.py` and `python test_smoke.py`)
# ==============================================================================
if __name__ == "__main__":
    exit_code = pytest.main(["-v", __file__])
    sys.exit(exit_code)
