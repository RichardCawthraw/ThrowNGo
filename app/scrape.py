from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import ElementNotInteractableException, ElementClickInterceptedException, NoSuchElementException
from time import sleep
from pymysql import connect
from contextlib import closing
from configparser import ConfigParser
from datetime import datetime


class Team:
    def __init__(self):
        self.app_conf = self.app_config()
        conn = self.db_connect(self.app_conf)
        drivers = self.get_drivers(conn)
        combo_ids = self.get_config(conn)
        print('\n*** SCRAPE ***')
        print('Start:\t' + str(datetime.now()))
        for combo in combo_ids['config_ids']:
            print('Config:\t' + combo)
            for driver in drivers:
                if driver['active']:
                    Driver(driver, combo, conn, self.app_conf)
        conn.close()
        print('End:\t' + str(datetime.now()))

    @staticmethod
    def app_config():
        config = ConfigParser()
        config.read('config.ini')
        return config

    @staticmethod
    def db_connect(config):
        host = config.get('DB', 'HOST')
        user = config.get('DB', 'USER')
        pwd = config.get('DB', 'PWD')
        db = config.get('DB', 'DB')
        conn = connect(host, user, pwd, db, autocommit=True)
        return conn

    @staticmethod
    def get_drivers(conn):
        drivers = []
        with closing(conn.cursor()) as cur:
            cur.execute('CALL usp_getDrivers()')
            rows = cur.fetchall()
            for row in rows:
                drivers.append({
                    'name': row[1],
                    'driver_id': row[2],
                    'active': True if row[3] == 1 else False
                })
        return drivers

    @staticmethod
    def get_config(conn):
        config = {}
        with closing(conn.cursor()) as cur:
            cur.execute('CALL usp_getWeekConfig()')
            row = cur.fetchone()
            if row:
                config = {
                    'week_num': row[1],
                    'config_ids': [config_id for config_id in row[2].split(',')],
                    'car': row[3],
                    'track': row[4],
                    'start': row[5],
                    'end': row[6]
                }
        return config


class Driver:
    def __init__(self, driver, combo, conn, app_conf):
        self.driver = driver
        self.combo = combo
        self.conn = conn
        self.app_conf = app_conf
        self.url = self.app_conf.get('VRS', 'URL')
        options = Options()
        options.headless = bool(int(self.app_conf.get('MISC', 'HEADLESS_BROWSER')))
        options.binary_location = self.app_conf.get('BIN', 'FF')
        try:
            web_driver = webdriver.Firefox(options=options, executable_path=self.app_conf.get('BIN', 'GD'))
            web_driver.get(self.url)
            sleep(int(self.app_conf.get('WAIT', 'LONG')))
            self.login(web_driver)
            self.get_session_links(web_driver)
        except BaseException as ex:
            print(ex)
        web_driver.quit()

    def click_identifier_id(self, driver):
        try:
            driver.find_element_by_id('identifierId').send_keys(self.app_conf.get('VRS', 'ACC'))
            return False
        except NoSuchElementException:
            return True

    def login(self, driver):
        """
        Log in to VRS using a Google account. User and pwd set in config.ini.
        """
        driver.find_element_by_class_name('card-action').find_element_by_tag_name('span').click()
        sleep(int(self.app_conf.get('WAIT', 'SHORT')))
        driver.find_element_by_id('gwt-debug-googleLogin').click()
        sleep(int(self.app_conf.get('WAIT', 'SHORT')))
        while self.click_identifier_id(driver):
            pass
        driver.find_element_by_id('identifierNext').click()
        sleep(int(self.app_conf.get('WAIT', 'LONG')))
        actions = ActionChains(driver)
        actions.send_keys(self.app_conf.get('VRS', 'PWD'))
        actions.perform()
        try:
            driver.find_element_by_id('passwordNext').click()
        except (ElementNotInteractableException, ElementClickInterceptedException):
            """ Retry as this often fails """
            sleep(int(self.app_conf.get('WAIT', 'LONG')))
            actions.send_keys(self.app_conf.get('VRS', 'PWD'))
            actions.perform()
            driver.find_element_by_id('passwordNext').click()
        print('Logged in as %s' % self.app_conf.get('VRS', 'ACC'))
        sleep(int(self.app_conf.get('WAIT', 'LONG')))

    def get_session_links(self, web_driver):
        print('Searching for TnG laps by %s' % self.driver['name'])
        full_url = '%s/#/Driver/%s/-1/%s' % (self.url, self.driver['driver_id'], self.combo)
        web_driver.get(full_url)
        sleep(int(self.app_conf.get('WAIT', 'SHORT')))
        try:
            elem = web_driver.find_element_by_css_selector('a[data-vrs-widget-field="viewDetails"]')
            elem.click()
            sleep(int(self.app_conf.get('WAIT', 'SHORT')))
            self.get_sessions(web_driver, self.driver)
        except (ElementNotInteractableException, NoSuchElementException, ElementNotInteractableException):
            """ Retry as this often fails """
            sleep(int(self.app_conf.get('WAIT', 'LONG')))
            try:
                elem = web_driver.find_element_by_class_name('icon-arrow-right-circle')
                elem.click()
                sleep(int(self.app_conf.get('WAIT', 'SHORT')))
                self.get_sessions(web_driver, self.driver)
            except (ElementNotInteractableException, NoSuchElementException, ElementNotInteractableException) as ex:
                if 'could not be scrolled into view' in ex.msg:
                    """ Element doesn't exist as driver hasn't done TnG """
                    print(self.driver['name'], 'has not participated in TnG this week')
                else:
                    print(ex)

    def get_sessions(self, web_driver, driver):
        num_of_sessions = len(web_driver.find_elements_by_css_selector('a[title="View Laps"]'))
        if num_of_sessions > 0:
            if web_driver.current_url.split('/')[7] == self.combo:
                self.get_stints(web_driver, num_of_sessions, driver)

    def get_stints(self, web_driver, num_of_sessions, driver):
        """ Loop through sessions and identify stints within """
        for i in range(num_of_sessions):
            session = web_driver.find_elements_by_css_selector('a[title="View Laps"]')[i]
            session.click()
            sleep(int(self.app_conf.get('WAIT', 'SHORT')))
            html = web_driver.page_source
            driver_id = web_driver.current_url.split('/')[5]
            config_id = web_driver.current_url.split('/')[7]
            soup, session_dt, sim_dt = self.get_session_meta(html)
            stints = soup.find_all('div', {'class': 'card-content'})[3:]
            for stint in stints:
                try:
                    stint_num = int(stint.find_all('span', {'class': 'card-title activator'})[0].find_all('span')[0].text.split(' ')[1])
                    self.get_stint_lap_times(driver_id, config_id, stint, stint_num, session_dt, sim_dt, web_driver, driver)
                except IndexError:
                    pass
            try:
                """ Try and navigate back to sessions page """
                back_btn = web_driver.find_element_by_css_selector('a[data-vrs-widget-field="up1LevelButton"]')
                back_btn.click()
            except ElementNotInteractableException as ex:
                print('Back btn exception')
                sleep(int(self.app_conf.get('WAIT', 'SHORT')))
                """ Retry as this often fails """
                back_btn = web_driver.find_element_by_css_selector('a[data-vrs-widget-field="up1LevelButton"]')
                back_btn.click()
            sleep(int(self.app_conf.get('WAIT', 'SHORT')))

    @staticmethod
    def get_session_meta(html):
        """
        Use BeautifulSoup4 to parse html and get session meta info (sim and IRL datetimes)
        """
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
        edited_dt_str = sim_dt_str[:len(sim_dt_str) - 8].strip() + (' %s:%s:00' % (str(hours).rjust(2, '0'), minutes_split[0]))
        try:
            sim_dt = datetime.strptime(edited_dt_str, '%B %d, %Y %H:%M:%S')
        except ValueError:
            sim_dt = datetime.strptime(edited_dt_str, '%b %d, %Y %H:%M:%S')
        return soup, session_dt, sim_dt

    def get_stint_lap_times(self, driver_id, config_id, stint, stint_num, session_dt, sim_dt, web_driver, driver):
        tbody = stint.find_all('tbody', {'data-vrs-widget': 'tbodyWrapper'})[1]
        lap_rows = tbody.find_all('tr')
        for lr in lap_rows:
            html = web_driver.page_source
            """ This attempts to validate that we're on the right driver's VRS page.
                Run into issues before with laps being written to wrong driver in db """
            valid_str = 'Driver(%s)/Platform' % driver['driver_id']
            if valid_str in html:
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
                        driver_id, config_id, sim_dt, session_dt, stint_num, lap_num,
                        lap_time, lap_state, air_temp, track_temp, fuel_used, fuel_left
                    ])
                    if int(cur.fetchone()[0]) == 1:
                        print('%s\tSession: %s\tStint: %s\tLap: %s\tTime: %s' % (
                            driver_id, str(session_dt), stint_num, lap_num, '%.3f' % lap_time))


Team()
