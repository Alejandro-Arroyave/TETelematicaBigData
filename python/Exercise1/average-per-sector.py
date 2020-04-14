from mrjob.job import MRJob


class AveragePerSector(MRJob):
    def mapper(self, _, line):
        _, ecosec, salary, _ = line.split(',')
        yield (int(ecosec), (int(salary), 1))

    def average(self, ecosec, salary):
        avg, count = 0, 0
        for tmp, c in salary:
            avg = (avg * count + tmp * c) / (count + c)
            count += c
        return (ecosec, avg)

    def reducer(self, ecosec, salary):
        ecosec, avg = self.average(ecosec, salary)
        yield (ecosec, avg)


if __name__ == '__main__':
    AveragePerSector.run()
