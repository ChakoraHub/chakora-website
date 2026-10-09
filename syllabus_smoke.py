import os
import pytest

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


BASE_URL = os.getenv(
    "SYLLABUS_BASE_URL",
    "https://www.chakorahub.com"
).rstrip("/")

SYLLABUS_URL = f"{BASE_URL}/syllabus"

WAIT = int(os.getenv("SELENIUM_WAIT_TIMEOUT", "25"))


@pytest.fixture
def driver():
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
    WebDriverWait(browser, WAIT).until(
        lambda d:
        d.execute_script("return document.readyState") == "complete"
    )


def test_syllabus_page_loads(driver):
    driver.get(SYLLABUS_URL)
    wait_loaded(driver)

    syllabus_content = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.ID, "syllabusContent")
        )
    )

    assert syllabus_content.is_displayed()


def test_syllabus_maintenance_notice(driver):
    expected = os.getenv(
        "EXPECT_MAINTENANCE_NOTICE",
        "false"
    ).lower() == "true"

    if not expected:
        pytest.skip("Maintenance notice is not expected")

    driver.get(SYLLABUS_URL)
    wait_loaded(driver)

    notice = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.ID, "syllabus-maintenance-notice")
        )
    )

    assert notice.is_displayed()
    assert "Scheduled maintenance notice" in notice.text