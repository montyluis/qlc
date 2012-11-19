from distutils.core import setup
setup(name='qlc',
    version='0.1.0',
    description='Quantitative Language Comparison',
    long_description='qlc is a library for quantitative language comparison. One purpose for exmaple is the computation of language trees by comparing word list and dictionary data.',
    author='Peter Bouda',
    author_email='pbouda@cidles.eu',
    url='https://github.com/pbouda/qlc',
    packages=[ 'qlc', 'qlc.comparison', 'qlc.distance', 'qlc.tests' ],
    package_dir={'qlc': 'src/qlc'},
    package_data={'qlc': ['data/asjp/listss13.txt', 'data/orthography_profiles/*.txt', 'data/stopwords/*.txt']},
)
