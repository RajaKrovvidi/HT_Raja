import io
import csv
from operator import add
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

f1 = "C:/Users/Surfer/Desktop/Har Tree/dataset1.csv"
f2 = "C:/Users/Surfer/Desktop/Har Tree/dataset2.csv"

def print_row(element):
  print (element)

def split_by_kv1(element, delimiter=","):
    # Need a better approach here
    splitted = element.split(delimiter)
    return (splitted[0], splitted[1], splitted[2]), element

def split_by_kv2(element, delimiter=","):
    # Need a better approach here
    splitted = element.split(delimiter)
    return splitted[0], splitted[1]



with beam.Pipeline() as p:
    ds1 = (p | "Read orders" >> beam.io.ReadFromText(f1, skip_header_lines=1)
              | "to KV order" >> beam.Map(split_by_kv1,  delimiter=",")
              )

    ds2 = (p | "Read users" >> beam.io.ReadFromText(f2, skip_header_lines=1)
             | "to KV users" >> beam.Map(split_by_kv2, delimiter=",")
             )
    ({"orders": ds1, "users": ds2} | beam.CoGroupByKey()
     | beam.Map(print) )



class LeftJoinerFn(beam.DoFn):
    def __init__(self):
        super(LeftJoinerFn, self).__init__()

    def process(self, row, **kwargs):
        rDict = dict(kwargs['right_list'])
        lKey = row[0][1]
        #print( 'In row ' + str(row) + 'Lkey is ' + str(lKey))
        if lKey in rDict:
            x = row[1]
            x.append(rDict[lKey])
            #print(x)
            yield (row[0], x)
        else:
            yield (row[0], row[1])

class GroupAndSum(beam.DoFn):
    def __init__(self):
        super(GroupAndSum, self).__init__()

    def process(self, row, **kwargs):
        key = row[0]
        status = row[1][0]
        if status == 'ARPA':
            row[1][0] = row[1][1]
            row[1][1] = 0
        else:
            row[1][0] = 0

        yield row

class RotateTier(beam.DoFn):

    def __init__(self):
        super(RotateTier, self).__init__()

    def process(self, row, **kwargs):
        y = row[1][-1:] + row[1][:-1]
        yield (row[0],y)

class Totals(beam.CombineFn):

    def create_accumulator(self):
        return []

    def add_input(self, accumulator, element):
        accumulator.append(element)
        return accumulator

    def merge_accumulators(self, accumulators, *args, **kwargs):

        vals = accumulators[0][1] if len(accumulators) and len(accumulators[0])>1 else []
        merged = [ 0 for i in range(len(vals) + 1)] if vals else []
        keyTotals = {}
        for a in accumulators:
            for item in a:
                merged = list( map(add, merged, item[1]) )
        if len(merged):
            accumulators[0].append((('Totals',None,None),merged))
        return accumulators[0]

    def extract_output(self, accumulator, *args, **kwargs):
        return accumulator

with beam.Pipeline() as p:
    ds1 = [(('L1', 'C1', 1), ['ARPA', 33]),
           (('L1', 'C1', 1), ['ARDA', 21]),
           (('L2', 'C2', 3), ['ARDA', 22]),
           (('L3', 'C3', 1), ['ARDA', 15]), ]

    ds2 = [('C1', 1), ('C2', 5), ('C3', 6)]

    left_joined = (
            ds1
            | 'Group and sum by status' >> beam.ParDo(GroupAndSum())
            | 'LeftJoiner: JoinValues' >> beam.ParDo(LeftJoinerFn(), right_list=ds2)
            | 'Move tier to front ' >> beam.ParDo(RotateTier())
            | 'Count per key ' >> beam.CombineGlobally(Totals())
            | 'Display' >> beam.Map(print_row)
    )

p.run()
