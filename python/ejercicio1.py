from mrjob.job import MRJob


class Ejercicio1(MRJob):
    def mapper(self, _, line):
        _, ecosec, salary, _ = line.split(',')
        yield (int(ecosec), (int(salary), 1))

    def _reducer_combiner(self, ecosec, salary):
        avg,count=0,0
        for ec,sa in salary:
            avg = (avg * count + ec * sa) / (count + sa)
            count += sa
            print('opt'+str(ec)+' '+str(sa))
        return (ecosec, (avg,count))

    def combiner(self, ecosec, salary):
        yield self._reducer_combiner(ecosec,salary)

    def reducer(self, ecosec, salary):
        ecosec, (avg,count) = self._reducer_combiner(ecosec, salary)
        yield (ecosec, avg)

    # def reducer(self, ecosec, salary):
    #     yield (ecosec, salary)


if __name__ == '__main__':
    Ejercicio1.run()
