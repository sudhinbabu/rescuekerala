# Generated by Django 2.1 on 2018-08-28 08:59
import os
from django.db import migrations, models, IntegrityError, transaction
import django.db.models.deletion
import csv
from mainapp.models import districts, LocalBody as ExLocalBody
from django.conf import settings


class Populate:
    valid_local_body_types = tuple([ex[0] for ex in ExLocalBody.local_body_types][:2])
    CSV_PATH = os.path.join(settings.BASE_DIR,
                            'mainapp/migrations/local_bodies.csv')

    def __call__(self, apps, schema_editor):
        self.apps = apps
        self.schema_editor = schema_editor
        self.db_alias = self.schema_editor.connection.alias
        self.populate_districts()
        self.populate_local_bodies()

    def populate_districts(self):
        District = self.apps.get_model("mainapp", "District")
        disctrict_objects = []
        for district in districts:
            name = district[1].split("-")[0].strip(" ")
            disctrict_objects.append(District(code=district[0],
                                              name=name,
                                              described_name=district[1]))

        District.objects.using(self.db_alias).bulk_create(disctrict_objects)

    def get_body_type(self, name):
        name = name.lower()
        for valid_type in self.valid_local_body_types:
            if valid_type in name:
                return valid_type
        return "panchayat"

    def local_bodies(self, district):
        with open(self.CSV_PATH, "r") as file:
            for local_body in csv.DictReader(file):
                if local_body[district]:
                    yield {"name": local_body[district],
                           "type": self.get_body_type(
                                        local_body[district])}

    def populate_local_bodies(self):
        District = self.apps.get_model("mainapp", "District")
        LocalBody = self.apps.get_model("mainapp", "LocalBody")
        for district in District.objects.all():
            for local_body in self.local_bodies(district.name):
                print(local_body)
                try:
                    with transaction.atomic():
                        LocalBody.objects.create(district=district,
                                                 body_type=local_body["type"],
                                                 name=local_body["name"])
                except IntegrityError:
                    print("duplicate found {}".format(local_body["name"]))


class Migration(migrations.Migration):

    dependencies = [
        ('mainapp', '0091_merge_20180825_1236'),
    ]

    operations = [
        migrations.CreateModel(
            name='District',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=150)),
                ('described_name', models.CharField(max_length=255)),
                ('code', models.CharField(max_length=3, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='LocalBody',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('body_type', models.CharField(choices=[('corporation', 'corporation'), ('muncipality', 'municipality'), ('panchayat', 'panchayat')], max_length=20)),
                ('name', models.CharField(max_length=200)),
                ('district', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='mainapp.District')),
            ],
        ),
        migrations.AlterUniqueTogether(
            name='localbody',
            unique_together={('name', 'district')},
        ),
        migrations.RunPython(Populate(), lambda apps, schema_editor: None)
    ]
