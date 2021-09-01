import _queue
import csv
import locale
import re
import threading, queue

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from webdriver_manager.utils import ChromeType


address2_regex = '^(.*)\s(\w{2})\s(\d+)-(\d+)'
valid_zip_codes = ['33470', '33411', '33412']

SOURCE_FILENAME = 'Indian Trail Improvement District Tax Roll FY2021.csv'
RESULT_FILENAME = 'appraiser_results.csv'
BROWSER_THREADS = 10

class AddressRecord(object):
    def __init__(self, address: str, address2: str):
        self._address = address
        self._address2 = address2

    @property
    def address(self):
        return self._address

    @property
    def address2(self):
        return self._address2


class Result(object):
    def __init__(self, address_record: AddressRecord, found: bool, taxable_value: float):
        self._found = found
        self._address_record = address_record
        self._taxable_value = taxable_value

    @property
    def address_record(self):
        return self._address_record

    @property
    def found(self):
        return self._found

    @property
    def taxable_value(self):
        return self._taxable_value


class PropertyEvaluatorThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None):
        threading.Thread.__init__(self, group=group, target=target, name=name, daemon=True)
        self.args = args
        self.kwargs = kwargs
        self._done = False

    @property
    def done(self):
        return self._done

    @done.setter
    def done(self, done):
        self._done = done

    def run(self):
        queue = self.args[0]
        results = self.args[1]
        chrome_options = Options()
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920x1080")
        driver = webdriver.Chrome(ChromeDriverManager(chrome_type=ChromeType.GOOGLE).install(), options=chrome_options)
        wait_driver = WebDriverWait(driver, 5)

        while True and not self.done:
            try:
                address_record = queue.get(timeout=5)

                match = re.match(address2_regex, address_record.address2)
                if match and match.group(3) in valid_zip_codes:
                    driver.get("https://www.pbcgov.org/papa/")
                    driver.switch_to.frame("master-search")
                    wait_driver.until(
                        expected_conditions.presence_of_element_located((By.ID, "txtSearch"))
                    )
                    search_field = driver.find_element_by_id("txtSearch")
                    search_field.clear()
                    search_field.send_keys(address_record.address)
                    search_field.send_keys(Keys.RETURN)

                    try:
                        wait_driver.until(expected_conditions.presence_of_element_located((By.XPATH, '//*[@id="content"]//legend[contains(text(), "Property Detail")]')))
                        taxable_values = driver.find_elements(By.XPATH, '//*[@id="tblAssVal"]//td[contains(text(), "Taxable Value")]/../td[@class="TDValueRight"]/span')
                        taxable_value_string = taxable_values[0].text
                        taxable_value = locale.atof(taxable_value_string.strip("$").replace(',', ''))
                        results.put(Result(address_record, found=True, taxable_value=taxable_value))

                    except TimeoutException:
                        results.put(Result(address_record, found=False, taxable_value=0))
                else:
                    results.put(Result(address_record, found=False, taxable_value=0))

            except _queue.Empty as qe:
                self.done = True
                continue

            queue.task_done()
        driver.close()


def write_results(results_queue: queue.Queue):
    with open(RESULT_FILENAME, mode='w') as csvfile:
        filewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(['Address', 'Address2', 'Found', '2021 Taxable Value'])
        while True:
            try:
                result = results_queue.get(timeout=30)
                print('Writing results to file...')
                filewriter.writerow([result.address_record.address, result.address_record.address2, result.found, result.taxable_value])
                results_queue.task_done()
            except _queue.Empty as qe:
                print('No more results to write to file.')
                break

q = queue.Queue()
rq = queue.Queue()

threads = []
for i in range(BROWSER_THREADS):
    thread = PropertyEvaluatorThread(name="{0}-Thread-{1}".format('LOXLIFE', i + 1), args=(q,rq))
    threads.append(thread)
    thread.start()

results_thread = threading.Thread(target=write_results, args=[rq])
results_thread.start()

with open(SOURCE_FILENAME, newline='') as csvfile:
    address_reader = csv.reader(csvfile, delimiter=',')
    for row in address_reader:
        search_address = row[1].strip()
        address2 = row[2].strip()
        address_record = AddressRecord(search_address, address2)
        q.put(address_record)

    print('All tasks requests sent\n', end='')
    q.join()
    print('All work is complete')
    for thread in threads:
        thread.done = True
        thread.join()

    results_thread.join()