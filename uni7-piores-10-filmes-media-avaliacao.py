from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import statistics

class Enciclopedia:
    movie_file = open("ml-100k/u.item")
    movies = csv.reader(movie_file, delimiter="|")

    def getMovieName(self, movie_id):
        for m in self.movies:
            if int(m[0]) == int(movie_id):
                return m[1]


class MRMostUsedWord(MRJob):

    contador = 0

    def mapper_get_movies(self, _, line):
        _,movie,rating,_ = line.split('\t')
        yield movie, int(rating)

    def reducer_calculate_ratings(self, movie, ratings):
        mean = statistics.mean(ratings)
        yield round(float(mean),2), movie

    def reducer_order(self, rating, movies):
        enc = Enciclopedia()
        for movie in movies:
            if MRMostUsedWord.contador < 10:
                MRMostUsedWord.contador = MRMostUsedWord.contador + 1
                yield rating, enc.getMovieName(movie)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_calculate_ratings),
            MRStep(reducer=self.reducer_order)
        ]

if __name__ == '__main__':
    MRMostUsedWord.run()