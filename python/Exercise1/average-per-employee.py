from mrjob.job import MRJob


class AveragePerEmployee(MRJob):
    def mapper(self, _, line):
        employee, _, salary, _ = line.split(',')
        yield (int(employee), (int(salary), 1))

    def average(self, employee, salary):
        avg, count = 0, 0
        for tmp, c in salary:
            avg = (avg * count + tmp * c) / (count + c)
            count += c
        return (employee, avg)

    def reducer(self, employee, salary):
        employee, avg = self.average(employee, salary)
        yield (employee, avg)


if __name__ == '__main__':
    AveragePerEmployee.run()
