import csv
import sqlite3


def queries(cur):
    cur.execute(""" SELECT      tailNumber, 
                                COUNT(*) 
                    FROM        FlightLeg 
                    GROUP BY    tailNumber 
                    ORDER BY    -COUNT(*)""")
    most_flights = cur.fetchone()
    print("The aircraft with ID: {0} made the most flights ({1})".format(most_flights[0], most_flights[1]))

    cur.execute(""" SELECT      tailNumber, 
                                flightDuration 
                    FROM        FlightLeg 
                    ORDER BY    -flightDuration """)
    most_time_flying = cur.fetchone()
    print("The aircraft with ID: {0} spent the most time in the air ({1} minutes)"
          .format(most_time_flying[0], most_time_flying[1]))

    cur.execute(""" SELECT      id, 
                                sourceAirportCode, 
                                sourceCountryCode, 
                                destinationAirportCode, 
                                flightDuration
                    FROM        FlightLeg
                    WHERE       flightType = 'D'
                    ORDER BY    flightDuration""")
    domestic_flights = cur.fetchall()
    print("The shortest domestic flight was from {0} to {1} in {2} (flight ID: {3}) and took {4} minutes"
          .format(domestic_flights[0][1], domestic_flights[0][3], domestic_flights[0][2],
                  domestic_flights[0][0], domestic_flights[0][4]))
    print("The longest domestic flight was from {0} to {1} in {2} (flight ID: {3}) and took {4} minutes"
          .format(domestic_flights[-1][1], domestic_flights[-1][3], domestic_flights[-1][2],
                  domestic_flights[-1][0], domestic_flights[-1][4]))

    cur.execute(""" SELECT      id, 
                                sourceAirportCode, 
                                sourceCountryCode, 
                                destinationAirportCode, 
                                destinationCountryCode, 
                                flightDuration
                    FROM        FlightLeg
                    WHERE       flightType = 'I'
                    ORDER BY    flightDuration""")
    international_flights = cur.fetchall()
    print("The shortest international flight was from {0} in {1} to {2} in {3} (flight ID: {4}) and took {5} minutes"
          .format(international_flights[0][1], international_flights[0][2], international_flights[0][3],
                  international_flights[0][4], international_flights[0][0], international_flights[0][5]))
    print("The longest international flight was from {0} in {1} to {2} in {3} (flight ID: {4}) and took {5} minutes"
          .format(international_flights[-1][1], international_flights[-1][2], international_flights[-1][3],
                  international_flights[-1][4], international_flights[-1][0], international_flights[-1][5]))

    cur.execute("ALTER TABLE FlightLeg ADD previousLandingTimeUtc")
    cur.execute(""" SELECT  LAG(landingTimeUtc, 1, 'None')
                            OVER (
                                PARTITION BY tailNumber 
                                ORDER BY tailNumber, departureTimeUtc)
                    FROM    FlightLeg""")
    faulty_records = cur.fetchall()
    cur.execute(""" SELECT      id 
                    FROM        FlightLeg
                    ORDER BY    tailNumber, departureTimeUtc 
                    """)
    ids = cur.fetchall()
    for every_id, every_record in zip(ids, faulty_records):
        cur.execute("UPDATE FlightLeg SET previousLandingTimeUtc = ? WHERE id = ?", [every_record[0], every_id[0]])
    cur.execute("ALTER TABLE FlightLeg ADD betweenLandingDeparture integer")
    cur.execute(""" UPDATE  FlightLeg 
                    SET     betweenLandingDeparture = 
                                ROUND((JULIANDAY(departureTimeUtc) - JULIANDAY(previousLandingTimeUtc))*1440)
                    """)
    cur.execute("SELECT * FROM FlightLeg WHERE betweenLandingDeparture < 0")
    all_rec = cur.fetchall()
    faulty_ids = []
    for a in all_rec:
        faulty_ids.append(a[0])
    print("Faulty records count in database: {0}. Problems occurred with the following flight ID's: {1}"
          .format(len(all_rec), faulty_ids))

    cur.execute(""" SELECT      id, 
                                tailNumber, 
                                betweenLandingDeparture
                    FROM        FlightLeg 
                    WHERE       betweenLandingDeparture > 0
                    ORDER BY    betweenLandingDeparture 
                    """)
    all_rec = cur.fetchone()
    print("""The shortest gap ({2} minutes) between landing and departure of the same plane 
occurred before flight ID: {0} of the aircraft number {1}""".format(all_rec[0], all_rec[1], all_rec[2]))


def main():
    con = sqlite3.connect("flights.db")
    cur = con.cursor()
    cur.execute(""" CREATE TABLE IF NOT EXISTS FlightLeg (
                        id INTEGER PRIMARY KEY, 
                        tailNumber, 
                        sourceAirportCode, 
                        sourceCountryCode, 
                        destinationAirportCode,
                        destinationCountryCode, 
                        departureTimeUtc, 
                        landingTimeUtc
                    );""")

    with open('flightlegs.csv', 'r', encoding='utf-8-sig') as file:
        read_file = csv.DictReader(file, delimiter=';')
        to_db = [(i['tailNumber'],
                  i['source_airport_code'],
                  i['source_country_code'],
                  i['destination_airport_code'],
                  i['destination_country_code'],
                  i['departure_time'],
                  i['landing_time']) for i in read_file]
        print(to_db)

    cur.executemany(""" INSERT INTO FlightLeg (
                            tailNumber, 
                            sourceAirportCode, 
                            sourceCountryCode, 
                            destinationAirportCode, 
                            destinationCountryCode, 
                            departureTimeUtc, 
                            landingTimeUtc
                        ) 
                        VALUES (?, ?, ?, ?, ?, ?, ?) """, to_db)

    cur.execute("ALTER TABLE FlightLeg ADD flightDuration integer")
    cur.execute(""" UPDATE  FlightLeg 
                    SET     flightDuration = ROUND((JULIANDAY(landingTimeUtc) - JULIANDAY(departureTimeUtc))*1440)""")
    cur.execute("ALTER TABLE FlightLeg ADD flightType")
    cur.execute(""" UPDATE  FlightLeg 
                    SET     flightType = CASE 
                    WHEN    sourceCountryCode = destinationCountryCode THEN 'D' ELSE 'I' END""")
    queries(cur)
    con.commit()
    con.close()


if __name__ == '__main__':
    main()
