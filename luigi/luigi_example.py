import luigi
import time


class HelloWorld(luigi.Task):
    a = luigi.parameter

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('helloworld.txt')

    def run(self):
        time.sleep(15)
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')
        time.sleep(15)


class NameSubstituter2(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return HelloWorld()

    def output(self):
        return luigi.LocalTarget(self.input().path + '.name_' + self.name)

    def run(self):
        time.sleep(15)
        with self.input().open() as infile, self.output().open('w') as outfile:
            text = infile.read()
            text = text.replace('World', self.name)
            outfile.write(text)
        time.sleep(15)


if __name__ == '__main__':
    luigi.run()
