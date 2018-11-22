from distutils.core import setup

setup(
    name='mlq',
    version='0.1.8',
    packages=['mlq', 'controller'],
    long_description=open('README.txt').read(),
    install_requires=open('requirements.txt').read(),
    include_package_data=True,
    url='https://github.com/tomgrek/mlq',
    author='Tom Grek',
    author_email='tom.grek@gmail.com'
)
