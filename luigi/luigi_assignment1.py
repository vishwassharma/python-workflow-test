import luigi
from os import path


# -------------- helper functions -----------------

def unzip(path_to_zip):
    import zipfile
    zip_ref = zipfile.ZipFile(path_to_zip, 'r')
    zip_ref.extractall(path.dirname(path_to_zip))
    zip_ref.close()
    return path.dirname(path_to_zip)


def processor_func(data):
    """
    Do some processing
    :param data:
    :return: processed data
    """

    processed_data = data
    return processed_data


class UnzipTask(luigi.Task):
    """
    Task to unzip the file
    """

    def requires(self):
        return None

    def run(self):
        unzip('output_ORIG.txt.zip')

    def output(self):
        return luigi.LocalTarget('output_ORIG.txt')


class FilterTask(luigi.Task):
    """
    Task to remove the redundant data
    """

    def requires(self):
        return UnzipTask()

    def output(self):
        return luigi.LocalTarget(self.input().path + "_filtered")

    def run(self):
        with self.input().open() as infile, self.output().open('w') as outfile:
            for line in infile.readlines():
                # print(line.split())
                if line.startswith('#'):
                    continue
                elif line.split()[1] == '3':
                    continue
                else:
                    outfile.write(line)


class ProcessingTask(luigi.Task):
    """
    Task to process data before output is published
    """

    def requires(self):
        return FilterTask()

    def output(self):
        input_path = self.input().path
        return luigi.LocalTarget(input_path.replace("filtered", "processed"))

    def run(self):
        with self.input().open() as infile, self.output().open('w') as outfile:
            outfile.write(processor_func(infile.read()))


class PublishingTask(luigi.Task):
    """
    Task to publish processed data
    """

    def requires(self):
        return ProcessingTask()

    def run(self):
        with self.input().open() as infile:
            print(infile.read())


if __name__ == "__main__":
    luigi.run()
