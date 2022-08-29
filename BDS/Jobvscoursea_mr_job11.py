from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (best_match_score, Title, Job_num, course_no, course_title, course_rating, course_students_enrolled) = line.split('\t')
        yield ("job_title:"+Title+":Course_Title:"+course_title), int(float(best_match_score)*100)

    def reducer_count_ratings(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    RatingsBreakdown.run()