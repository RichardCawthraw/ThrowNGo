import discord
from pymysql import connect
from contextlib import closing
from datetime import datetime
from subprocess import Popen
from json import dumps
from configparser import ConfigParser


class ThrowNGo(discord.Client):
    @staticmethod
    def db_connect(conf):
        host = conf.get('DB', 'HOST')
        user = conf.get('DB', 'USER')
        pwd = conf.get('DB', 'PWD')
        db = conf.get('DB', 'DB')
        conn = connect(host, user, pwd, db, autocommit=True)
        return conn

    async def on_ready(self):
        print('Logged on as', self.user)
        self.config = ConfigParser()
        self.config.read('C:\Source\ThrowNGo\config.ini')

    async def on_message(self, message):
        channel = client.get_channel(int(self.config.get('DISCORD', 'CHANNEL')))
        if message.author == self.user:
            return
        if message.content.lower().startswith('!tng'):
            self.get_championship_standings()
            if message.content.lower().startswith('!tng scrape'):
                driver_name = message.content.lower().replace('!tng scrape', '').strip()
                txt = self.scrape_driver(driver_name)
            elif message.content.lower().startswith('!tng leaderboard_week_'):
                try:
                    week_num = int(message.content.split('_')[3])
                    season = int(message.content.split('_')[2])
                    txt = self.get_past_leaderboard(season, week_num)
                except BaseException:
                    txt = 'Invalid request format'
            elif message.content.lower().startswith('!tng leaderboard'):
                txt = self.get_leaderboard()
            elif message.content.lower().startswith('!tng standings'):
                txt = self.get_standings()
            elif message.content.lower().startswith('!tng live_standings'):
                txt = self.get_live_standings()
            elif message.content.lower().startswith('!tng latest_session'):
                driver_name = message.content.lower().replace('!tng latest_session', '').strip()
                txt = self.get_latest_session_by_driver(driver_name)
            elif message.content.lower().startswith('!tng lap_count_'):
                try:
                    week_num = int(message.content.split('_')[3])
                    season = int(message.content.split('_')[2])
                    txt = self.get_past_lap_counter(season, week_num)
                except BaseException:
                    txt = 'Invalid request format'
            elif message.content.lower().startswith('!tng lap_count'):
                txt = self.get_lap_counter()
            else:
                txt = "Usage: !tng [request]\n\n" \
                      "where requests include:\n" \
                      "\tscrape <name>\t\t\t\t\t\tStarts a web scrape for that driver\n" \
                      "\tleaderboard\t\t\t\t\t\t\tThe current week's leaderboard\n" \
                      "\tstandings\t\t\t\t\t\t\t\tThe championship standings\n" \
                      "\tlive\_standings\t\t\t\t\t\tThe live championship standings\n" \
                      "\tlatest\_session <name>\t\tLaps from <name>'s last session\n" \
                      "\tlap\_count\t\t\t\t\t\t\t\tLaps done this week\n" \
                      "\tleaderboard\_week\_<season>\_<week>\tPast week's leaderboard\n" \
                      "\tlap\_count\_week\_<season>\_<week>\tPast week's lap count"
            await channel.send(txt)

    def get_latest_session_by_driver(self, driver_name):
        conn = self.db_connect(self.config)
        if driver_name == 'the chris':
            driver_name = 'chris sewell'
        with closing(conn.cursor()) as cur:
            cur.execute('CALL usp_getLatestSession(%s)', [driver_name])
            rows = cur.fetchall()
        if not rows:
            return None
        body = "```glsl\nSession Start Time:  %s\n\nStint | Lap | Time | Track Temp | Gap to Leader\n" % rows[0][4]
        for row in rows:
            lap_row = "%s  %s  %s  %s  %s\n" % (
                str(row[5]).ljust(2, ' '),
                str(row[6]).ljust(2, ' '),
                self.format_lap(row[8]).ljust(10, ' '),
                str(int(round(row[10], 0))).ljust(2, ' ') + 'C',
                '+' + str(row[14]).ljust(5, '0')
            )
            body += lap_row
        txt = (body + "```")
        if len(txt) > 2000:
            txt = txt[:1997] + '```'
        return txt

    def get_lap_counter(self):
        conn = self.db_connect(self.config)
        with closing(conn.cursor()) as cur:
            cur.execute('CALL usp_getLapCounter()')
            rows = cur.fetchall()
        if not rows:
            return None
        body = "```glsl\nClean Laps | Total Laps | Clean %\n"
        for row in rows:
            output = "%s  %s  %s  %s\n" % (str(row[1]).rjust(3, ' '), str(row[2]).rjust(3, ' '), f'{row[3]:.3f}', row[0])
            body += output
        txt = body + '```'
        return txt

    def get_past_lap_counter(self, season, week_num):
        conn = self.db_connect(self.config)
        with closing(conn.cursor()) as cur:
            cur.execute('CALL usp_getPastWeekConfig(%s, %s)', [season, week_num])
            row = cur.fetchone()
            cur.execute('CALL usp_getPastLapCounter(%s, %s)', [season, week_num])
            rows = cur.fetchall()
        if not rows:
            return 'There is no data for this week'
        head = "```css\nSeason:   %s\nWeek:   %s\nTrack:  %s\nCar:    %s\n```\n" % (row[7], row[1], row[4], row[3])
        body = "```glsl\nClean Laps | Total Laps | Clean %\n"
        for row in rows:
            output = "%s  %s  %s  %s\n" % (str(row[1]).rjust(3, ' '), str(row[2]).rjust(3, ' '), f'{row[3]:.3f}', row[0])
            body += output
        txt = head + body + '```'
        return txt        

    def get_live_standings(self):
        conn = self.db_connect(self.config)
        with closing(conn.cursor()) as cur:
            cur.execute('CALL usp_getLiveStandings()')
            rows = cur.fetchall()
        if not rows:
            return None
        pos = 1
        body = "```md\n"
        for row in rows:
            pos_format = str(pos).ljust(2, ' ')
            pts = str(row[1]).ljust(4, ' ') if row[1] else '0   '
            result_row = "#%s  %s  %s\n" % (pos_format, pts, row[0])
            body += result_row
            pos += 1
        txt = body + '```'
        return txt

    def get_standings(self):
        conn = self.db_connect(self.config)
        with closing(conn.cursor()) as cur:
            cur.execute('CALL usp_getStandings()')
            rows = cur.fetchall()
        if not rows:
            return 'There are no current standings available'
        pos = 1
        body = "```md\n"
        for row in rows:
            result_row = "#%s  %s %s\n" % (str(pos).ljust(4, ' '), str(row[1]).ljust(3, ' '), row[0])
            body += result_row
            pos += 1
        txt = (body + "```")
        return txt

    def get_leaderboard(self):
        conn = self.db_connect(self.config)
        with closing(conn.cursor()) as cur:
            cur.execute('CALL usp_getWeekConfig()')
            config = cur.fetchone()
            cur.execute('CALL usp_getLeaderboard()')
            rows = cur.fetchall()
        if not rows:
            return 'There is no leaderboard data'
        head = "```css\nWeek:   %s\nTrack:  %s\nCar:    %s\n```\n" % (config[1], config[4], config[3])
        body = "```md\n"
        for row in rows:
            blap = self.format_lap(row[3])
            gap = self.format_gap(row[4])
            pts = '0' if row[1] is None else row[1]
            result_row = "#%s %s  %s  %s  %s\n" % (
                str(row[0]).ljust(3, ' '), str(pts).ljust(2, ' '), blap, gap.ljust(7, ' '), row[2])
            body += result_row
        txt = (head + body + "```")
        return txt

    @staticmethod
    def format_lap(blap):
        minutes = str(int(blap / 60))
        seconds_fl = blap % 60
        seconds = f'{seconds_fl:.3f}'
        if seconds[1] == '.':
            seconds = '0' + seconds
        return '%s:%s' % (minutes, seconds)

    @staticmethod
    def format_gap(gap):
        if gap == 0:
            return '      '
        return '+' + f'{gap:.3f}'

    def get_championship_standings(self):
        conn = self.db_connect(self.config)
        with closing(conn.cursor()) as cur:
            cur.execute('CALL usp_getWeekConfig()')
            row = cur.fetchone()
            if not row:
                cur.execute('CALL usp_getMaxWeekAndSeason()')
                row = cur.fetchone()
                week = row[0] + 1
                season = row[1]
            else:
                week = row[1]
                season = row[7]
            for i in range(1, week):
                cur.execute('CALL usp_getPastWeekByWeekNum(%s, %s)', [i, season])
                rows = cur.fetchall()
                for row in rows:
                    pts = row[2] if row[2] else 0
                    week_num, rank, driver_id = row[0], row[1], row[3]
                    cur.execute('CALL usp_writePastResult(%s, %s, %s, %s, %s)', [
                        week_num, rank, pts, driver_id, season
                    ])

    def get_past_leaderboard(self, season, week_num):
        conn = self.db_connect(self.config)
        with closing(conn.cursor()) as cur:
            cur.execute('CALL usp_getPastWeekConfig(%s, %s)', [season, week_num])
            config = cur.fetchone()
            cur.execute('CALL usp_getPastWeekResults(%s, %s)', [season, week_num])
            rows = cur.fetchall()
        if not rows:
            return 'There is no data for this week'
        head = "```css\nSeason:   %s\nWeek:   %s\nTrack:  %s\nCar:    %s\n```\n" % (config[7]. config[1], config[4], config[3])
        body = "```md\n"
        for row in rows:
            blap = self.format_lap(row[3])
            gap = self.format_gap(row[4])
            pts = '0' if row[1] is None else row[1]
            result_row = "#%s  %s  %s  %s\t%s\n" % (
                row[0], str(pts).ljust(2, ' '), blap, gap, row[2])
            body += result_row
        txt = (head + body + "```")
        return txt

    def scrape_driver(self, driver_name):
        conn = self.db_connect(self.config)
        with closing(conn.cursor()) as cur:
            cur.execute('CALL usp_getDriverIDByName(%s)', [driver_name])
            row = cur.fetchone()
        if row[0] == 0:
            return 'No driver found'
        elif row[0] > 1:
            return 'Multiple matches found, please be more specific'
        driver_id = row[1]
        driver_full_name = row[2]
        json_driver = dumps({
            'name': driver_full_name,
            'driver_id': driver_id
        })
        py_path = self.config.get('BIN', 'PY')
        Popen(args=[py_path, 'scrape_driver.py', json_driver])
        txt = 'VRS scrape started for %s' % driver_full_name
        return txt


config = ConfigParser()
config.read('C:\Source\ThrowNGo\config.ini')
token = config.get('DISCORD', 'TOKEN')
client = ThrowNGo()
client.run(token)
