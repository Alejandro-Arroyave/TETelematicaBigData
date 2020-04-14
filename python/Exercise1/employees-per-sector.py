from mrjob.job import MRJob

class EmployeesPerSector(MRJob):

    def mapper(self, _, line):
        employee, sector, _, _ = line.split(',')
        yield (int(sector), (int(employee),1))

    def cont(self, sector, employee):
        cont = 0
        for emp,c in employee:
            cont += c
        return (sector, cont)

    def reducer(self, sector, employee):
        employee, cont = self.cont(sector, employee)
        yield (sector, cont)

if __name__ == '__main__':
    EmployeesPerSector.run()