import sys
import argparse
import pandas as pd
from pandas import DataFrame

def load_inputs_and_merge(file1, file2):
    ds1 = pd.read_csv(file1)
    ds2 = pd.read_csv(file2)
    return ds1.merge(ds2)

def group_and_sum(df):

    dsRtg = df.groupby( ["legal_entity", "counter_party", "tier"])[["rating"]].max()
    dt1 = df.groupby( ["legal_entity", "counter_party", "tier", "status"])[["value"]].sum()

    # pivot to see by status type
    result = DataFrame(pd.pivot_table(dt1, values="value", index= ["legal_entity", "counter_party", "tier"] , columns= ["status"]  ))
    # Add rating to the df
    result["maxRtg"] = dsRtg
    cols = list(result)
    # Add ratings column
    cols.insert(0, cols.pop(cols.index("maxRtg")))
    # move it the front of status columns
    result = result.loc[:, cols]
    result = result.fillna(0.0)

    # get totals
    totals = result.groupby(['legal_entity']).sum()

    for i in list(totals['maxRtg'].keys()):
        result.loc[(i + ' Total',  '', '')] = [totals['maxRtg'][i], totals['ACCR'][i], totals['ARAP'][i]]

    result = result.sort_index()
    bigTotal = totals.sum()
    result.loc[('Total',  '', '')] = [bigTotal['maxRtg'], bigTotal['ACCR'], bigTotal['ARAP']]
    return result

def persist_result(result, outpath):
    result.to_csv(outpath)

def process(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input1',
        dest='fp1',
        default="C:/Users/Surfer/Desktop/Har Tree/dataset1.csv",
        help='Input file1 to process.')
    parser.add_argument(
        '--input2',
        dest='fp2',
        default="C:/Users/Surfer/Desktop/Har Tree/dataset2.csv",
        help='Input file1 to process.')
    parser.add_argument(
        '--output',
        dest='outpath',
        default = "C:/Users/Surfer/Desktop/Har Tree/result1.csv",
        help='Output file to write results to.')

    args, pps = parser.parse_known_args(argv)

    df = load_inputs_and_merge(args.fp1, args.fp2)
    result = group_and_sum(df)
    persist_result(result, args.outpath)
    print(result)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    process(sys.argv[1:])

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
