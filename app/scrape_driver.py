from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import ElementNotInteractableException, ElementClickInterceptedException, NoSuchElementException
from time import sleep
from datetime import datetime, timedelta
from pymysql import connect
from contextlib import closing
from json import loads
from sys import argv
from configparser import ConfigParser


class ScrapeDriver:
    def __init__(self, driver_name, driver_id):
        self.app_config = self.app_config()
        self.driver_name = driver_name
        self.driver_id = driver_id
        self.url = self.app_config.get('VRS', 'URL')
        self.success = False

        attempts = 0
        while attempts < 3 and not self.success:
            attempts += 1
            self.conn = self.db_connect(self.app_config)
            self.config = self.get_config()
            options = Options()
            options.headless = bool(self.app_config.get('MISC', 'HEADLESS_BROWSER'))
            options.binary_location = self.app_config.get('BIN', 'FF')
            try:
                web_driver = webdriver.Firefox(options=options, executable_path=self.app_config.get('BIN', 'GD'))
                web_driver.get('%s/#/Driver/-1' % self.url)
                sleep(int(self.app_config.get('WAIT', 'LONG')))
                self.login(web_driver)
                self.get_session_links(web_driver)
                self.success = True
            except BaseException as ex:
                print('%s: %s; %s' % (str(datetime.now()), self.driver_name, ex))
        web_driver.close()
        self.conn.close()
        print('%s: Scrape complete for %s' % (str(datetime.now()), self.driver_name))

    @staticmethod
    def app_config():
        config = ConfigParser()
        config.read('C:\Source\ThrowNGo\config.ini')
        return config

    @staticmethod
    def db_connect(config):
        host = config.get('DB', 'HOST')
        user = config.get('DB', 'USER')
        pwd = config.get('DB', 'PWD')
        db = config.get('DB', 'DB')
        conn = connect(host, user, pwd, db, autocommit=True)
        return conn

    def get_config(self):
        config = {}
        with closing(self.conn.cursor()) as cur:
            cur.execute('CALL usp_getWeekConfig()')
            row = cur.fetchone()
            if row:
                config = {
                    'week_num': row[1],
                    'config_id': row[2],
                    'car': row[3],
                    'track': row[4],
                    'start': row[5],
                    'end': row[6]
                }
        return config

    def login(self, web_driver):
        web_driver.find_element_by_xpath("//span[text()='Login']").click()
        sleep(int(self.app_config.get('WAIT', 'SHORT')))

        web_driver.find_element_by_id('email').click()
        user = ActionChains(web_driver)
        user.send_keys(self.app_config.get('VRS', 'USER'))
        user.perform()

        web_driver.find_element_by_id('password').click()
        pwd = ActionChains(web_driver)
        pwd.send_keys(self.app_config.get('VRS', 'PWD'))
        pwd.perform()

        web_driver.find_element_by_xpath("//button[@type='submit']").click()
        sleep(int(self.app_config.get('WAIT', 'SHORT')))

    def get_session_links(self, web_driver):
        print('%s:\tSearching for laps by %s' % (str(datetime.now()), self.driver_name))
        full_url = '%s/#/Driver/%s/-1/%s' % (self.url, self.driver_id, self.config['config_id'])
        web_driver.get(full_url)
        sleep(int(self.app_config.get('WAIT', 'SHORT')))
        try:
            elem = web_driver.find_element_by_css_selector('a[data-vrs-widget-field="viewDetails"]')
            elem.click()
            sleep(int(self.app_config.get('WAIT', 'SHORT')))
            self.get_sessions(web_driver, self.config['config_id'])
        except (ElementNotInteractableException, NoSuchElementException, ElementNotInteractableException):
            try:
                elem = web_driver.find_element_by_class_name('icon-arrow-right-circle')
                elem.click()
                sleep(int(self.app_config.get('WAIT', 'SHORT')))
                self.get_sessions(web_driver, self.config['config_id'])
            except (ElementNotInteractableException, NoSuchElementException, ElementNotInteractableException) as ex:
                if 'could not be scrolled into view' in ex.msg:
                    print(self.driver_name, 'has not participated in TnG this week')
                else:
                    print(ex)
                return False
        return True

    def get_sessions(self, web_driver, config_id):
        num_of_sessions = len(web_driver.find_elements_by_css_selector('a[title="View Laps"]'))
        if num_of_sessions > 0:
            if web_driver.current_url.split('/')[7] == config_id:
                self.get_stints(web_driver, num_of_sessions)

    def get_stints(self, web_driver, num_of_sessions):
        for i in range(num_of_sessions):
            session = web_driver.find_elements_by_css_selector('a[title="View Laps"]')[i]
            session.click()
            sleep(int(self.app_config.get('WAIT', 'SHORT')))
            html = web_driver.page_source
            url_driver_id = web_driver.current_url.split('/')[5]
            url_config_id = web_driver.current_url.split('/')[7]
            soup = BeautifulSoup(html, features='html.parser')
            session_dt_str = soup.find_all('div', {'data-vrs-widget': 'SessionInfoPanel'})[0] \
                .find_all('span', {'data-vrs-widget-field': 'date'})[0] \
                .find_all('span')[0].text
            session_dt = datetime.strptime(session_dt_str, '%Y-%m-%d %H:%M')
            sim_dt_str = soup.find('h4', text='Time Of Day').find_next('span').find_all('span')[0]['title']
            time_part = sim_dt_str[len(sim_dt_str) - 8:].split(':')
            minutes_split = time_part[1].split(' ')
            hours = int(time_part[0].strip()) if minutes_split[1] == 'am' else int(time_part[0].strip()) + 12
            hours = 0 if hours == 24 else hours
            edited_dt_str = sim_dt_str[:len(sim_dt_str) - 8].strip() + (' %s:%s:00' % (str(hours), minutes_split[0]))
            sim_dt = datetime.strptime(edited_dt_str, '%b %d, %Y %H:%M:%S')
            stints = soup.find_all('div', {'class': 'card-content'})[3:]
            for stint in stints:
                stint_num = int(stint.find_all('span', {'class': 'card-title activator'})[0].find_all('span')[0].text.split(' ')[1])
                self.get_stint_lap_times(url_driver_id, url_config_id, stint, stint_num, session_dt, sim_dt)
            try:
                back_btn = web_driver.find_element_by_css_selector('a[data-vrs-widget-field="up1LevelButton"]')
                back_btn.click()
            except ElementNotInteractableException as ex:
                print('Back btn exception')
                sleep(int(self.app_config.get('WAIT', 'SHORT')))
                back_btn = web_driver.find_element_by_css_selector('a[data-vrs-widget-field="up1LevelButton"]')
                back_btn.click()
            sleep(int(self.app_config.get('WAIT', 'SHORT')))

    def get_stint_lap_times(self, url_driver_id, config_id, stint, stint_num, session_dt, sim_dt):
        tbody = stint.find_all('tbody', {'data-vrs-widget': 'tbodyWrapper'})[1]
        lap_rows = tbody.find_all('tr')
        for lr in lap_rows:
            cells = lr.find_all('td')
            lap_num = int(cells[0].find_all('h3')[0].text.split(' ')[0])
            lap_state = cells[1].find_all('h3')[0]['title']
            lap_time_str = cells[2].find_all('h3')[0].text
            if ':' in lap_time_str:
                lap_time = (int(lap_time_str.split(':')[0]) * 60) + float(lap_time_str.split(':')[1])
            else:
                lap_time = float(lap_time_str.replace('s', ''))
            air_temp = float(cells[3].find_all('h3')[0].text.split(' ')[0])
            track_temp = float(cells[3].find_all('h3')[1].text.split(' ')[0])
            fuel_used = cells[4].find_all('h3')[0].text.split(' ')[0] # Not to float as can be empty string on VRS
            fuel_used = 0 if fuel_used == '' else fuel_used
            fuel_left = cells[4].find_all('h3')[1].text.split(' ')[0]
            with closing(self.conn.cursor()) as cur:
                cur.execute('CALL usp_addLap(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', [
                    url_driver_id, config_id, sim_dt, session_dt, stint_num, lap_num,
                    lap_time, lap_state, air_temp, track_temp, fuel_used, fuel_left
                ])
                if int(cur.fetchone()[0]) == 1:
                    print('%s\tSession: %s\tStint: %s\tLap: %s\tTime: %s' % (
                        url_driver_id, str(session_dt), stint_num, lap_num, '%.3f' % lap_time))


driver = loads(argv[1])
ScrapeDriver(driver['name'], driver['driver_id'])


