import luigi
import os
import pandas as pd

class CalculateAveragePoint(luigi.Task):
    input_file = luigi.Parameter()

    def output(self):
        output_file = self.input_file.replace("submits", "results")
        return luigi.LocalTarget(output_file)

    def run(self):
        df = pd.read_csv(self.input_file)
        df.convert_dtypes()
        df["average"] = (df["math_point"] + df["literature_point"] + df["english_point"]) / 3

        # Save
        output_file = self.input_file.replace("submits", "results")
        df.to_csv(output_file)
        os.remove(self.input_file)

