import setuptools

with open("README.md", "r", encoding='utf-8') as fh:
  long_description = fh.read()

setuptools.setup(
  name="FineCache",
  version="0.0.2",
  author="Ciaran Chen",
  author_email="ciaranchen@qq.com",
  description="A function cache for fine-tuning",
  long_description=long_description,
  long_description_content_type="text/markdown",
  url="https://github.com/ciaranchen/FineCache",
  packages=setuptools.find_packages(),
  classifiers=[
  "Programming Language :: Python :: 3",
  "License :: OSI Approved :: MIT License",
  "Operating System :: OS Independent",
  ],
)