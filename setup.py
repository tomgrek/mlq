from distutils.core import setup

setup(
    name='mlq',
    version='0.1.6',
    packages=['mlq', 'controller'],
    long_description=open('README.txt').read(),
    install_requires=open('requirements.txt').read(),
    url='https://github.com/tomgrek/mlq',
    author='Tom Grek',
    author_email='tom.grek@gmail.com'
)
