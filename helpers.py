def compute_avg_query_time_ms(time, runs):
    return (time/runs) * 1000

import csv
def write_data_to_csv(data, filename):
    with open(filename,'w') as out:
        csv_out=csv.writer(out)
        csv_out.writerow(['table','query','time(ms)'])
        for row in data:
            csv_out.writerow(row)